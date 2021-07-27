/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <list>
#include <map>
#include <string>
#include <algorithm>
#include <fstream>
#include <io.h>

#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif

typedef struct _Condition {
	std::string name;
	int op; // 1 for >, 2 for <, 3 fro =, 4 is null 5 is not null
	int vt; // 1 for int, 2 for string
	int dv;
	std::string sv;

} Condition;

typedef struct _Vec {
	int size;
	char* data;
	int record_size;

} Vec;

typedef struct _Table {
	table_file_header header;
	Vec data;
} Table;

typedef int (*Compare)(void* a, void* b);

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;
	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}
	rc = initialize_tpd_list();
  	if (rc)
  	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
  	}
	else
	{
	  	g_sql = argv[1];
    	rc = get_token(argv[1], &tok_list);
		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			tmp_tok_ptr = tok_ptr->next;
			free(tok_ptr);
			tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

int save_tpd_list_stream(FILE* f, tpd_list* tl) {
	fwrite(tl, tl->list_size, 1, f);
	return 0;
}

void rename_log() {
	char ln[64];
	int i = 0;
	do {
	i++;
	sprintf(ln, "db.log%d", i);
	} while (!_access(ln, 0));
	rename(DB_LOG, ln);
}

int save_tpd_list(char* name, tpd_list* tl) {
	FILE* handle = fopen(name, "wbc");
	if (handle == NULL) {
		return -1;
	}
	save_tpd_list_stream(handle, tl);
	fclose(handle);

	return 0;
}

int load_log(const char* name, std::list<std::string>& logs) {
	if (_access(name, 0)) {
		std::ofstream out(name);
		out.close();
	}
	std::ifstream file;
	file.open(name);
	if (!file.is_open()) {
		return -1;
	}
	while (!file.eof()) {
		std::string line;
		std::getline(file, line);
		if (!line.empty()) {
			logs.push_back(line);
		}
	}
	file.close();

	return 0;
}

int save_log(const char* name, std::list<std::string>& logs) {
	std::ofstream file(name, std::ios::out| std::ios::trunc);
	if (!file.is_open()) {
		return -1;
	}
	for (auto& log : logs) {
		file << log << std::endl;
	}
	file.close();

	return 0;
}

int init_vec(Vec* vec, int record_size) {
	vec->size = 0;
	vec->data = NULL;
	vec->record_size = record_size;

	return 0;
}

int init_table(Table* table, int record_size) {
	table->header.file_size = sizeof(table_file_header) + 100 * record_size;
	table->header.file_header_flag = 0;
	table->header.num_records = 0;
	table->header.record_offset = sizeof(table_file_header);
	table->header.record_size = record_size;
	table->header.tpd_ptr = 0;
	init_vec(&table->data, record_size);

	return 0;
}

int free_vec(Vec* vec) {
	free(vec->data);
	vec->data = NULL;
	return 0;
}

void resize(Vec* vec, int new_size) {
	if (vec->size < new_size) {
		vec->data = (char*)realloc(vec->data, new_size * vec->record_size);
		vec->size = new_size;
	}
}
char* get_index(Vec* vec, int i) {
	if (i >= vec->size) {
		return  NULL;
	} 

	return vec->data + vec->record_size * i;
}

int add_vec(Vec* vec, void* item) {
	resize(vec, vec->size + 1);
	memcpy(get_index(vec, vec->size - 1), item, vec->record_size);
	return 0;
}

void* find_vec(Vec* vec, void* item ,Compare f) {
	int i = 0;
	void* p = NULL;
	for (i = 0; i < vec->size; i++) {
		if (f( get_index(vec, i), item) == 0) {
			return get_index(vec, i);
		}
	}

	return NULL;
}

int del_vec(Vec* vec, void* item, Compare f) {
	int i = 0;
	int flag = -1;
	for (i = 0; i < vec->size; i++) {
		if (f( get_index(vec, i), item) == 0) {
			if (i < (vec->size - 2)) {
				memcpy(get_index(vec, i), get_index(vec, vec->size - 1), vec->record_size);
			}	
			resize(vec, vec->size - 1);
			flag = 0;
		}
	}
	
	return flag;
}

int load_table(char* name, Table* t) {
	char tname[128] = {0};
	FILE* thandle = NULL;
	sprintf(tname, "%s.tab", name);
	int i = 0;
	char* buff = NULL;
	if ((thandle = fopen(tname, "rbc")) == NULL) {
		return FILE_OPEN_ERROR;
	}
	fread(&t->header, sizeof(t->header), 1, thandle);
	t->header.tpd_ptr = get_tpd_from_list(name);
	if (!t->header.tpd_ptr) {
		fclose(thandle);
		return TABLE_NOT_EXIST;
	}
	init_vec(&t->data, t->header.record_size);
	buff = (char*)calloc(1, t->header.record_size);
	for (i = 0; i < t->header.num_records; i++) {
		fread(buff, t->header.record_size, 1, thandle);
		add_vec(&t->data, buff);
	}
	free(buff);
	fclose(thandle);
	
	return 0;
}

int load_table_stream(char* name, Table* t, FILE* thandle) {
	int i = 0;
	char* buff = NULL;
	int size = 0;
	fread(&size, sizeof(int), 1, thandle);
	int header_size = sizeof(t->header);
	fread(&t->header, header_size, 1, thandle);
	t->header.tpd_ptr = get_tpd_from_list(name);
	if (!t->header.tpd_ptr) {
		fclose(thandle);
		return TABLE_NOT_EXIST;
	}
	init_vec(&t->data, t->header.record_size);
	buff = (char*)calloc(1, t->header.record_size);
	for (i = 0; i < t->header.num_records; i++) {
		fread(buff, t->header.record_size, 1, thandle);
		add_vec(&t->data, buff);
	}
	free(buff);
	
	return 0;
}

int save_table(char* name, Table* t) {
	char tname[128] = {0};
	FILE* thandle = NULL;
	sprintf(tname, "%s.tab", name);
	int i = 0;
	char* buff = NULL;
	tpd_entry* ptr = t->header.tpd_ptr;
	if ((thandle = fopen(tname, "wbc")) == NULL) {
		return FILE_OPEN_ERROR;
	}
	t->header.tpd_ptr = 0;
	int header_size = sizeof(t->header);
	fwrite(&t->header, header_size, 1, thandle);
	fwrite(t->data.data, t->header.record_size, t->data.size, thandle);
	fclose(thandle);

	return 0;
}

int get_table_size(Table* t) {
	int res = 0;
	res += sizeof(t->header);
	res += (t->header.record_size * t->data.size);

	return res;
}

int save_table_stream(char* name, Table* t, FILE* thandle) {
	int i = 0;
	char* buff = NULL;
	tpd_entry* ptr = t->header.tpd_ptr;
	int size = sizeof(t->header) + t->header.record_size * t->data.size;
	t->header.tpd_ptr = 0;
	fwrite(&size, sizeof(int), 1, thandle);
	int header_size = sizeof(t->header);
	fwrite(&t->header, header_size, 1, thandle);
	fwrite(t->data.data, t->header.record_size, t->data.size, thandle);
	
	return 0;
}

int add_table(Table* table, void* item) {
	add_vec(&table->data, item);
	table->header.num_records++;

	return 0;
}

int del_compare(void* a, void* b) {
	return a == b ? 0 : -1;
}

int del_table(Table* table, void* item) {
	if (del_vec(&table->data, item, del_compare) == 0) {
		table->header.num_records--;
		return 0;
	}
	else {
		return -1;
	}
}
	
/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            t_class = function_name;
          else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
  token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
		((cur->next != NULL) && (cur->next->tok_value == K_INTO)) )
	{
		printf("INSERT statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_SELECT )
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if (cur->tok_value == K_DELETE) {
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next;
	
	}
	else if (cur->tok_value == K_UPDATE) {
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	
	}
	else if (cur->tok_value == K_BACKUP && cur->next != NULL  
			&& cur->next->tok_value == K_TO) {
		printf("BACKUP TO statement\n");
		cur_cmd = BACKUP;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_RESOTRE && cur->next != NULL  
			&& cur->next->tok_value == K_FROM) {
		printf("RESTORE statement\n");
		cur_cmd = RESTORE;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_ROLLFORWARD) 
	{
		printf("ROLLFORWARD statement\n");
		cur_cmd = ROLLFORWARD;
		cur = cur->next;
	
	}
	else
    {
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	std::list<std::string> logs;
	char times[64];
	std::string sql = g_sql;
	get_now(times);
	std::string time_str = times;
	time_str = time_str + " ";

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
			case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
			case LIST_TABLE:
						sql = "";
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						sql = "";
						rc = sem_list_schema(cur);
						break;
			case INSERT:
				rc = sem_insert_table(cur);
				break;
			case SELECT:
			    sql = "";
				rc = sem_select_table(cur);
				break;
			case DELETE:
				rc = sem_delete_table(cur);
				break;
			case UPDATE:
				rc = sem_update_table(cur);
				break;
			case BACKUP:
				time_str = "";
				sem_backup(cur);
				break;
			case RESTORE:
				time_str = "";
			    sql = "";
				sem_restore(cur);
				break;
			case ROLLFORWARD:
			    sql = "";
				sem_rollforward(cur);
				break;
			default:
					; /* no action */
		}
	}
	if (rc == 0) {
		if (!sql.empty()) {
			std::string log = time_str + sql;
			load_log(DB_LOG, logs);
			logs.push_back(log);
			save_log(DB_LOG, logs);
		}
	}
	
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];

	if (g_tpd_list->db_flags != 0) {
		return INVALID_STATEMENT;
	}

	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
		                  {
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char() or varchar() 
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
															 sizeof(cd_entry) *	tab_entry.num_columns;
				  tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{




						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						FILE* thandle = NULL;
						char tname[128] = {0};
						sprintf(tname, "%s.tab", new_entry->table_name);
						int record_size = 0;
						table_file_header header;
						char* buffer = NULL;
						record_size = cal_record_size(new_entry);
						header.file_size = sizeof(table_file_header) + 100 * record_size;
						header.file_header_flag = 0;
						header.num_records = 0;
						header.record_offset = sizeof(table_file_header);
						header.record_size = record_size;
						header.tpd_ptr = 0;
						buffer = (char*)calloc(100, record_size );


						thandle = fopen(tname, "wbc");
						if (thandle == NULL) {
							return FILE_OPEN_ERROR;
						}
						fwrite(&header, sizeof(header), 1, thandle);
						fwrite(buffer, record_size, 100, thandle);
						free(buffer);
						fclose(thandle);
						

						free(new_entry);
					}
				}
			}
		}
	}
  return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	if (g_tpd_list->db_flags != 0) {
		return INVALID_STATEMENT;
	}

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				char tname[128] = { 0 };
				sprintf(tname, "%s.tab", tab_entry->table_name);
				remove(tname);
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nNo Table Defined!\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("|==========Start==========|\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("|===========End===========|\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}
int sem_insert_table(token_list* t_list) {
	Table tab;
	int offset = 0;
	token_list* cur = t_list;
	tpd_entry* entry = NULL;
	cd_entry* cd = NULL;
	char* table = NULL;
	int index = 0;
	int record_size = 0;
	char* record = NULL;

	if (g_tpd_list->db_flags != 0) {
		return INVALID_STATEMENT;
	}
	if (cur == NULL ||  cur->tok_class != identifier) {
		return INVALID_STATEMENT;
	}

	table = cur->tok_string;

	cur = cur->next;

	if (cur == NULL ||  cur->tok_value != K_VALUES) {
		return INVALID_STATEMENT;
	}

	cur = cur->next;

	if (cur == NULL || cur->tok_value != S_LEFT_PAREN) {
		return INVALID_STATEMENT;
	}

	cur = cur->next;

	
	if (load_table(table, &tab)!= 0){
		return INVALID_STATEMENT;
	}

	entry = tab.header.tpd_ptr;
	record = (char*)calloc(1, tab.header.record_size);
	offset = entry->num_columns;

	while (cur != NULL && index < entry->num_columns) {
		if (cur->tok_value == S_COMMA) {
			cur = cur->next;
			continue;
		}

		cd = get_cd_from_table(entry, index);
		if (!cd) {
			free(record);
			return INVALID_STATEMENT;
		}

		if (cur->tok_value == K_NULL) {

			if (cd->not_null == 1) {
				free(record);
				return INVALID_STATEMENT;
			}

			record[index] = 0;
		} else {

			record[index] = cd->col_len;
			if (cd->col_type == T_INT) {

				if (cur->tok_value != INT_LITERAL) {
				
					printf("Integer Type Error: %d\n", cur->tok_value);
					free(record);
					return INVALID_STATEMENT;
				}

				int v = atoi(cur->tok_string);
				memcpy(record + offset, &v, sizeof(v));
			}
			else {
				if (cur->tok_value != STRING_LITERAL) {
					free(record);
					printf("String Type Error: %d\n", cur->tok_value);
					return INVALID_STATEMENT;
				}

				strncpy(record + offset, cur->tok_string, cd->col_len);
			}

		}

		offset += cd->col_len;
		cur = cur->next;
		index++;
	}

	if (cur == NULL || cur->tok_value != S_RIGHT_PAREN) {
		free(record);
		return INVALID_STATEMENT;
	}

	add_table(&tab, record);

	free(record);

	if (save_table(table, &tab) != 0) {
		return INVALID_STATEMENT;
	}

	printf("%s.tab new size = %d\n", table, get_table_size(&tab));
	return 0;
	
}
int sem_update_table(token_list* t_list) {
	token_list* cur = t_list;
	char* table = NULL;
	Table tab;
	int ret = 0;
	int i = 0;
	int j = 0;
	tpd_entry* entry = NULL;
	int offset = 0;
	char* record = NULL;
	cd_entry* cd = NULL;
	int type = -1; // 1 for col, 2 for sum, 3 for avg, 4 for count,  5 for star
	std::map<std::string, int> cols;
	std::string sum;
	std::string avg;
	std::string count;
	std::string star;
	std::list<Condition> conds;
	std::list<Condition> sets;
	int cond_type = -1;
	int orderby = 1;
	std::string order_name;

	if (g_tpd_list->db_flags != 0) {
		return INVALID_STATEMENT;
	}



	if (cur == NULL || cur->tok_class != identifier) {
		return INVALID_STATEMENT;
	}
	table = cur->tok_string;

	ret = load_table(table, &tab);
	if (ret != 0) {
		return ret;
	}

	cur = cur->next;

	if (cur == NULL || cur->tok_value != K_SET) {
		return INVALID_STATEMENT;
	}

	cur = cur->next;

	while (cur != NULL && cur->tok_value != K_WHERE) {
		Condition item;

		if (cur ==NULL || cur->tok_class != identifier) {
			return INVALID_STATEMENT;
		}
		item.name = cur->tok_string;
		cur = cur->next;

		if (cur ==NULL || cur->tok_value != S_EQUAL) {
			return INVALID_STATEMENT;
		}

		cur = cur->next;

		if (cur == NULL || (cur->tok_value != INT_LITERAL && cur->tok_value != STRING_LITERAL && cur->tok_value != K_NULL)) {
			return INVALID_STATEMENT;
		}

		if (cur->tok_value == INT_LITERAL) {
			item.dv = atoi(cur->tok_string);
			item.vt = INT_LITERAL;
		}
		else if (cur->tok_value == STRING_LITERAL){
			item.sv = cur->tok_string;
			item.vt = STRING_LITERAL;
		
		}
		else {
			item.sv = cur->tok_string;
			item.vt = K_NULL;
		
		}

		sets.push_back(item);

		cur = cur->next;
		
		if (cur != NULL && cur->tok_value == S_COMMA) {
			cur = cur->next;
		}

	}

	



	if (cur != NULL && cur->tok_value == K_WHERE) {
		Condition cond1;
		cur = cur->next;
		if (cur == NULL || cur->tok_class != identifier) {
			return INVALID_STATEMENT;
		}
		cond1.name = cur->tok_string;
		cur = cur->next;
		if (cur == NULL || (cur->tok_value != K_IS && (cur->tok_value < S_EQUAL || cur->tok_value > S_GREATER))) { return INVALID_STATEMENT;
			return INVALID_STATEMENT;
		}

		if (cur->tok_value == K_IS) {
			token_list* pre = cur;
			cur = cur->next;
			if (cur != NULL && cur->tok_value == K_NOT) {
					
				cond1.op = 5;
			}
			else {
				cond1.op = 4;
				cur = pre;
			}
		}
		else if (cur->tok_value == S_EQUAL) {
			cond1.op = 3;
		}
		else if (cur->tok_value == S_GREATER) {
			cond1.op = 1;
		}
		else {
			cond1.op = 2;
		}

		cur = cur->next;
		if (cur == NULL || (cur->tok_value != INT_LITERAL && cur->tok_value != STRING_LITERAL)) {
			return INVALID_STATEMENT;
		}

		if (cur->tok_value == INT_LITERAL) {
			cond1.vt = INT_LITERAL;
			cond1.dv = atoi(cur->tok_string);
		}
		else {
			cond1.vt = STRING_LITERAL;
			cond1.sv = cur->tok_string;
		}

		cur = cur->next;
		conds.push_back(cond1);
		if (cur != NULL && (cur->tok_value == K_OR || cur->tok_value == K_AND)) {
			if (cur->tok_value == K_OR) {
				cond_type = K_OR;
			}
			else {
				cond_type = K_AND;
			}
			cur = cur->next;
			if (cur == NULL || cur->tok_class != identifier) {
				return INVALID_STATEMENT;
			}
			cond1.name = cur->tok_string;
			cur = cur->next;
			if (cur == NULL || (cur->tok_value != K_IS &&(cur->tok_value < S_EQUAL || cur->tok_value > S_GREATER))) { return INVALID_STATEMENT;
					return INVALID_STATEMENT;
			}

			if (cur->tok_value == K_IS) {
				token_list* pre = cur;
				cur = cur->next;
				if (cur != NULL && cur->tok_value == K_NOT) {
						
					cond1.op = 5;
				}
				else {
					cond1.op = 4;
					cur = pre;
				}
			}
			else if (cur->tok_value == S_EQUAL) {
				cond1.op = 3;
			}
			else if (cur->tok_value == S_GREATER) {
				cond1.op = 1;
			}
			else {
				cond1.op = 2;
			}

			cur = cur->next;
			if (cur == NULL ||( cur->tok_value != INT_LITERAL && cur->tok_value != STRING_LITERAL)) {
				return INVALID_STATEMENT;
			}

			if (cur->tok_value == INT_LITERAL) {
				cond1.vt = INT_LITERAL;
				cond1.dv = atoi(cur->tok_string);
			}
			else {
				cond1.vt = STRING_LITERAL;
				cond1.sv = cur->tok_string;
			}

			cur = cur->next;
			conds.push_back(cond1);
			}
	}


	entry = tab.header.tpd_ptr;
	std::vector<char*> records;
	for (i = 0; i < tab.header.num_records; i++) {
		record = get_index(&tab.data, i);
		bool match[2] = { true, true };
		int index = 0;
		for (auto&& cond : conds) {
			offset = entry->num_columns;
			for (j = 0; j < entry->num_columns; j++) {
				cd = get_cd_from_table(entry, j);
				if (cond.name == cd->col_name) {
					if (cond.op == 4) {
						if (record[j] == 0) {
							match[index] = true;
						}
						else {
							match[index] = false;
						}
					}
					else if (cond.op == 5) {
						if (record[j] != 0) {
							match[index] = true;
						}
						else {
							match[index] = false;
						}
					}
					else {
						if (record[j] == 0) {
							match[index] = false;
						}
						else {
							if (cd->col_type == T_INT) {
								if (cond.op == 1) {
									match[index] = (*(int*)(record + offset)) > cond.dv;
								}
								else if (cond.op == 2) {
									match[index] = (*(int*)(record + offset)) < cond.dv;
								
								}
								else {
									match[index] = (*(int*)(record + offset)) == cond.dv;
								}
							}
							else {

								if (cond.op == 1) {
									match[index] = strcmp((record + offset), cond.sv.c_str()) > 0;
								}
								else if (cond.op == 2) {
									match[index] = strcmp((record + offset), cond.sv.c_str()) < 0;
								
								}
								else {
									match[index] = strcmp((record + offset), cond.sv.c_str()) == 0;
								}
							
							}
						}
						
					
					}
						
				}
				offset += cd->col_len;
			}

			index++;
		}

		bool res_match = match[0];
		for (size_t i = 1; i < conds.size(); i++) {
			if (cond_type == K_OR) {
				res_match = res_match || match[i];
			}
			else {
				res_match = res_match && match[i];
			}
		}

		if (res_match) {
			records.push_back(record);
		}
	}

	if (records.size() == 0) {
		printf("WARNING: NO ITEM UPDATED!\n");
	}
	else {

		for (auto& record : records) {
			for (auto& s : sets) {
			offset = entry->num_columns;
			int index = 0;
			for (j = 0; j < entry->num_columns; j++) {
				cd = get_cd_from_table(entry, j);
				if (s.name == cd->col_name) {
					record[index] = 1;
					if (s.vt == INT_LITERAL) {
						*((int*)(record + offset)) = s.dv;
					}
					else {
						if (s.vt == K_NULL) {
							record[index] = 0;
						}
						else {
							strncpy(record + offset, s.sv.c_str(), cd->col_len);
						}
					}
				}
				offset += cd->col_len;
				index++;
				}	
			}
		
		}

		save_table(table, &tab);
	}	
	free_vec(&tab.data);

	return 0;

}
int sem_delete_table(token_list* t_list) {
	token_list* cur = t_list;
	char* table = NULL;
	Table tab;
	int ret = 0;
	int i = 0;
	int j = 0;
	tpd_entry* entry = NULL;
	int offset = 0;
	char* record = NULL;
	cd_entry* cd = NULL;
	int type = -1; // 1 for col, 2 for sum, 3 for avg, 4 for count,  5 for star
	std::map<std::string, int> cols;
	std::string sum;
	std::string avg;
	std::string count;
	std::string star;
	std::list<Condition> conds;
	int cond_type = -1;
	int orderby = 1;
	std::string order_name;

	if (g_tpd_list->db_flags != 0) {
		return INVALID_STATEMENT;
	}


	if (cur == NULL || cur->tok_value != K_FROM) {
		return INVALID_STATEMENT;
	}

	cur = cur->next;

	if (cur == NULL || cur->tok_class != identifier) {
		return INVALID_STATEMENT;
	}
	table = cur->tok_string;

	ret = load_table(table, &tab);
	if (ret != 0) {
		return ret;
	}

	cur = cur->next;
	if (cur != NULL && cur->tok_value == K_WHERE) {
		Condition cond1;
		cur = cur->next;
		if (cur == NULL || cur->tok_class != identifier) {
			return INVALID_STATEMENT;
		}
		cond1.name = cur->tok_string;
		cur = cur->next;
		if (cur == NULL || (cur->tok_value != K_IS && (cur->tok_value < S_EQUAL || cur->tok_value > S_GREATER))) { return INVALID_STATEMENT;
			return INVALID_STATEMENT;
		}

		if (cur->tok_value == K_IS) {
			token_list* pre = cur;
			cur = cur->next;
			if (cur != NULL && cur->tok_value == K_NOT) {
					
				cond1.op = 5;
			}
			else {
				cond1.op = 4;
				cur = pre;
			}
		}
		else if (cur->tok_value == S_EQUAL) {
			cond1.op = 3;
		}
		else if (cur->tok_value == S_GREATER) {
			cond1.op = 1;
		}
		else {
			cond1.op = 2;
		}

		cur = cur->next;
		if (cur == NULL || (cur->tok_value != INT_LITERAL && cur->tok_value != STRING_LITERAL)) {
			return INVALID_STATEMENT;
		}

		if (cur->tok_value == INT_LITERAL) {
			cond1.vt = INT_LITERAL;
			cond1.dv = atoi(cur->tok_string);
		}
		else {
			cond1.vt = STRING_LITERAL;
			cond1.sv = cur->tok_string;
		}

		cur = cur->next;
		conds.push_back(cond1);
		if (cur != NULL && (cur->tok_class == K_OR || cur->tok_class == K_AND)) {
			if (cur->tok_class == K_OR) {
				cond_type = K_OR;
			}
			else {
				cond_type = K_AND;
			}
			cur = cur->next;
			if (cur == NULL || cur->tok_class != identifier) {
				return INVALID_STATEMENT;
			}
			cond1.name = cur->tok_string;
			cur = cur->next;
			if (cur == NULL || (cur->tok_value != K_IS &&(cur->tok_value < S_EQUAL || cur->tok_value > S_GREATER))) { return INVALID_STATEMENT;
					return INVALID_STATEMENT;
			}

			if (cur->tok_value == K_IS) {
				token_list* pre = cur;
				cur = cur->next;
				if (cur != NULL && cur->tok_value == K_NOT) {
						
					cond1.op = 5;
				}
				else {
					cond1.op = 4;
					cur = pre;
				}
			}
			else if (cur->tok_value == S_EQUAL) {
				cond1.op = 3;
			}
			else if (cur->tok_value == S_GREATER) {
				cond1.op = 1;
			}
			else {
				cond1.op = 2;
			}

			cur = cur->next;
			if (cur == NULL ||( cur->tok_value != INT_LITERAL && cur->tok_value != STRING_LITERAL)) {
				return INVALID_STATEMENT;
			}

			if (cur->tok_value == INT_LITERAL) {
				cond1.vt = INT_LITERAL;
				cond1.dv = atoi(cur->tok_string);
			}
			else {
				cond1.vt = STRING_LITERAL;
				cond1.sv = cur->tok_string;
			}

			cur = cur->next;
			conds.push_back(cond1);
			}
	}


	entry = tab.header.tpd_ptr;
	std::vector<char*> records;
	for (i = 0; i < tab.header.num_records; i++) {
		record = get_index(&tab.data, i);
		bool match[2] = { true, true };
		int index = 0;
		for (auto&& cond : conds) {
			offset = entry->num_columns;
			for (j = 0; j < entry->num_columns; j++) {
				cd = get_cd_from_table(entry, j);
				if (cond.name == cd->col_name) {
					if (cond.op == 4) {
						if (record[j] == 0) {
							match[index] = true;
						}
						else {
							match[index] = false;
						}
					}
					else if (cond.op == 5) {
						if (record[j] != 0) {
							match[index] = true;
						}
						else {
							match[index] = false;
						}
					}
					else {
						if (record[j] == 0) {
							match[index] = false;
						}
						else {
							if (cd->col_type == T_INT) {
								if (cond.op == 1) {
									match[index] = (*(int*)(record + offset)) > cond.dv;
								}
								else if (cond.op == 2) {
									match[index] = (*(int*)(record + offset)) < cond.dv;
								
								}
								else {
									match[index] = (*(int*)(record + offset)) == cond.dv;
								}
							}
							else {

								if (cond.op == 1) {
									match[index] = strcmp((record + offset), cond.sv.c_str()) > 0;
								}
								else if (cond.op == 2) {
									match[index] = strcmp((record + offset), cond.sv.c_str()) < 0;
								
								}
								else {
									match[index] = strcmp((record + offset), cond.sv.c_str()) == 0;
								}
							
							}
						}
						
					
					}
						
				}
				offset += cd->col_len;
			}

			index++;
		}

		bool res_match = match[0];
		for (size_t i = 1; i < conds.size(); i++) {
			if (cond_type == K_OR) {
				res_match = res_match || match[i];
			}
			else {
				res_match = res_match && match[i];
			}
		}

		if (res_match) {
			records.push_back(record);
		}
	}

	if (records.size() == 0) {
		printf("WARNING: NO ITEM DELETED!\n");
	}
	else {

		Table new_table;
		init_table(&new_table, tab.header.record_size);
		for (i = 0; i < tab.header.num_records; i++) {
			record = get_index(&tab.data, i);
			int flag = 0;
			for (auto& item : records) {
				if (item == record) {
					flag = 1;
					break;
				}
			}

			if (flag == 0) {
				add_table(&new_table, record);
			}
		}
		save_table(table, &new_table);
	}
	free_vec(&tab.data);

	return 0;
}

int sem_select_table(token_list* t_list) {
	token_list* cur = t_list;
	char* table = NULL;
	Table tab;
	int ret = 0;
	int i = 0;
	int j = 0;
	tpd_entry* entry = NULL;
	int offset = 0;
	char* record = NULL;
	cd_entry* cd = NULL;
	int type = -1; // 1 for col, 2 for sum, 3 for avg, 4 for count,  5 for star
	std::map<std::string, int> cols;
	std::list<std::string> seq_cols;
	std::string sum;
	std::string avg;
	std::string count;
	std::string star;
	std::list<Condition> conds;
	int cond_type = -1;
	int orderby = 1;
	std::string order_name;

	while (cur != NULL && cur->tok_value != K_FROM) {
		int check = 0;
		if (cur->tok_value == S_STAR) {
			star = "*";
			if (type < 0) {
				type = 5;
			}
			else {
				return INVALID_STATEMENT;
			}

			cur = cur->next;
		}
		else if (cur->tok_value == F_SUM) {
			if (type != -1) {
				return INVALID_STATEMENT;
			}
			cur = cur->next;
			if (cur == NULL || cur->tok_value != S_LEFT_PAREN) {
				return INVALID_STATEMENT;
			}
			cur = cur->next;
			if (cur == NULL || cur->tok_class != identifier) {
				return INVALID_STATEMENT;
			}
			sum = cur->tok_string;
			cur = cur->next;
			if (cur == NULL || cur->tok_value != S_RIGHT_PAREN) {
				return INVALID_STATEMENT;
			}

			cur = cur->next;
			type = 2;

		}
		else if (cur->tok_value == F_AVG) {
			if (type != -1) {
				return INVALID_STATEMENT;
			}
			cur = cur->next;
			if (cur == NULL || cur->tok_value != S_LEFT_PAREN) {
				return INVALID_STATEMENT;
			}
			cur = cur->next;
			if (cur == NULL || cur->tok_class != identifier) {
				return INVALID_STATEMENT;
			}
			avg = cur->tok_string;
			cur = cur->next;
			if (cur == NULL || cur->tok_value != S_RIGHT_PAREN) {
				return INVALID_STATEMENT;
			}
			cur = cur->next;
			type = 3;
		}
		else if (cur->tok_value == F_COUNT) {
			if (type != -1) {
				return INVALID_STATEMENT;
			}
			cur = cur->next;
			if (cur == NULL || cur->tok_value != S_LEFT_PAREN) {
				return INVALID_STATEMENT;
			}
			cur = cur->next;
			if (cur == NULL || (cur->tok_class != identifier && cur->tok_value != S_STAR)) {
				return INVALID_STATEMENT;
			}
			count = cur->tok_string;
			cur = cur->next;
			if (cur == NULL || cur->tok_value != S_RIGHT_PAREN) {
				return INVALID_STATEMENT;
			}
			cur = cur->next;
			type = 4;
		}
		else {
			if (type != -1 && type != 1) {
				return INVALID_STATEMENT;
			}
			type = 1;

			if (cur->tok_class == identifier) {
				cols[std::string(cur->tok_string)] = 1;
				seq_cols.push_back(cur->tok_string);
			}
			else {
				return INVALID_STATEMENT;
			}

			cur = cur->next;
			if (cur != NULL && cur->tok_value == S_COMMA) {
				cur = cur->next;
			}
		}
	
	}

	if (cur == NULL || cur->tok_value != K_FROM) {
		return INVALID_STATEMENT;
	}

	cur = cur->next;

	if (cur == NULL || cur->tok_class != identifier) {
		return INVALID_STATEMENT;
	}
	table = cur->tok_string;

	ret = load_table(table, &tab);
	if (ret != 0) {
		return ret;
	}

	cur = cur->next;
	if (cur != NULL && cur->tok_value == K_WHERE) {
		Condition cond1;
		cur = cur->next;
		if (cur == NULL || cur->tok_class != identifier) {
			return INVALID_STATEMENT;
		}
		cond1.name = cur->tok_string;
		cur = cur->next;
		if (cur == NULL || cur->tok_value != K_IS &&(cur->tok_value < S_EQUAL || cur->tok_value > S_GREATER)) { return INVALID_STATEMENT;
			return INVALID_STATEMENT;
		}

		if (cur->tok_value == K_IS) {
			token_list* pre = cur;
			cur = cur->next;
			if (cur != NULL && cur->tok_value == K_NOT) {
					
				cond1.op = 5;
			}
			else {
				cond1.op = 4;
				cur = pre;
			}
		}
		else if (cur->tok_value == S_EQUAL) {
			cond1.op = 3;
		}
		else if (cur->tok_value == S_GREATER) {
			cond1.op = 1;
		}
		else {
			cond1.op = 2;
		}

		cur = cur->next;
		if (cur == NULL || (cur->tok_value != INT_LITERAL && cur->tok_value != STRING_LITERAL)) {
			return INVALID_STATEMENT;
		}

		if (cur->tok_value == INT_LITERAL) {
			cond1.vt = INT_LITERAL;
			cond1.dv = atoi(cur->tok_string);
		}
		else {
			cond1.vt = STRING_LITERAL;
			cond1.sv = cur->tok_string;
		}

		cur = cur->next;
		conds.push_back(cond1);
		if (cur != NULL && (cur->tok_value == K_OR || cur->tok_value == K_AND)) {
			if (cur->tok_value == K_OR) {
				cond_type = K_OR;
			}
			else {
				cond_type = K_AND;
			}
			cur = cur->next;
			if (cur == NULL || cur->tok_class != identifier) {
				return INVALID_STATEMENT;
			}
			cond1.name = cur->tok_string;
			cur = cur->next;
			if (cur == NULL || (cur->tok_value != K_IS && (cur->tok_value < S_EQUAL || cur->tok_value > S_GREATER))) { return INVALID_STATEMENT;
					return INVALID_STATEMENT;
			}

			if (cur->tok_value == K_IS) {
				token_list* pre = cur;
				cur = cur->next;
				if (cur != NULL && cur->tok_value == K_NOT) {
						
					cond1.op = 5;
				}
				else {
					cond1.op = 4;
					cur = pre;
				}
			}
			else if (cur->tok_value == S_EQUAL) {
				cond1.op = 3;
			}
			else if (cur->tok_value == S_GREATER) {
				cond1.op = 1;
			}
			else {
				cond1.op = 2;
			}

			cur = cur->next;
			if (cur == NULL || (cur->tok_value != INT_LITERAL && cur->tok_value != STRING_LITERAL)) {
				return INVALID_STATEMENT;
			}

			if (cur->tok_value == INT_LITERAL) {
				cond1.vt = INT_LITERAL;
				cond1.dv = atoi(cur->tok_string);
			}
			else {
				cond1.vt = STRING_LITERAL;
				cond1.sv = cur->tok_string;
			}

			cur = cur->next;
			conds.push_back(cond1);
			}
	}

	if (cur != NULL && cur->tok_value == K_ORDER) {
		cur = cur->next;
		if (cur == NULL || cur->tok_value != K_BY) {
			return INVALID_STATEMENT;
		}

		cur = cur->next;
		if (cur == NULL || cur->tok_class != identifier) {
			return INVALID_STATEMENT;
		}

		order_name = cur->tok_string;

		cur = cur->next;
		if (cur != NULL && cur->tok_value == K_DESC) {
			orderby = -1;
			cur = cur->next;
		}
	}

	int sum_value = 0;
	int count_value = 0;

	entry = tab.header.tpd_ptr;
	std::vector<char*> records;
	for (i = 0; i < tab.header.num_records; i++) {
		record = get_index(&tab.data, i);
		bool match[2] = { true, true };
		int index = 0;
		for (auto&& cond : conds) {
			offset = entry->num_columns;
			for (j = 0; j < entry->num_columns; j++) {
				cd = get_cd_from_table(entry, j);
				if (cond.name == cd->col_name) {
					if (cond.op == 4) {
						if (record[j] == 0) {
							match[index] = true;
						}
						else {
							match[index] = false;
						}
					}
					else if (cond.op == 5) {
						if (record[j] != 0) {
							match[index] = true;
						}
						else {
							match[index] = false;
						}
					}
					else {
						if (record[j] == 0) {
							match[index] = false;
						}
						else {
							if (cd->col_type == T_INT) {
								int val = (*(int*)(record + offset));
								if (cond.op == 1) {
									match[index] = val > cond.dv;
								}
								else if (cond.op == 2) {
									match[index] = val < cond.dv;
								
								}
								else {
									match[index] = val == cond.dv;
								}
							}
							else {
								char* val = (record + offset);
								if (cond.op == 1) {
									match[index] = strncmp(val, cond.sv.c_str(), cd->col_len) > 0;
								}
								else if (cond.op == 2) {
									match[index] = strncmp(val, cond.sv.c_str(), cd->col_len) < 0;
								
								}
								else {
									match[index] = strncmp(val, cond.sv.c_str(), cd->col_len) == 0;
								}
							
							}
						}
						
					
					}
						
				}
			offset += cd->col_len;
			}

			index++;
		}

		bool res_match = match[0];
		for (size_t i = 1; i < conds.size(); i++) {
			if (cond_type == K_OR) {
				res_match = res_match || match[i];
			}
			else {
				res_match = res_match && match[i];
			}
		}

		if (res_match) {
			records.push_back(record);
		}
	}

	if (!order_name.empty()) {
		
		std::sort(records.begin(), records.end(), [&](char* r1, char* r2) {
			int offset = entry->num_columns;
			bool res = true;
			for (j = 0; j < entry->num_columns; j++) {
				cd = get_cd_from_table(entry, j);
				if (order_name == cd->col_name) {
					if (r1[j] == 0) {
						res = true;
					}
					else if (r2[j] == 0) {
						res  = false;
					}
					else if (cd->col_type == T_INT) {
						res = *(int*)(r1 + offset) < *(int*)(r2 + offset);
					}
					else {
					
						res = strcmp(r1 + offset, r2 + offset) < 0;
					}

					if (orderby < 0) {
						res = !res;
					}

					return res;
				}
				offset += cd->col_len;
			}

			return true;
		});
		
	}


	int sum_val = 0;
	int count_val = 0;

	if (star == "*") {
		for (int i = 0; i < entry->num_columns; i++) {
			cd = get_cd_from_table(entry, i);
				printf("%-20s", cd->col_name);
		}
	}
	else if (seq_cols.size() > 0) {
		for (auto& it : seq_cols) {
				printf("%-20s", it.c_str());
		}
	}

	printf("\n");
	printf("|=====================================================================================|\n");

	for (i = 0; i < records.size(); i++) {
		offset = entry->num_columns;
		record = records[i];
		int new_line = 0;

		std::map<std::string, std::string> row_map;

		for (j = 0; j < entry->num_columns; j++) {

			cd = get_cd_from_table(entry, j);
			if (star == "*" || cols.find(cd->col_name) != cols.end()) {
				if (record[j] == 0) {
					if (star == "*") {
						printf("%-10s", "NULL");
					}
					else {
						row_map[cd->col_name] = "NULL";
					}
				}
				else {
					if (cd->col_type == T_INT) {
						if (star == "*") {
							printf("%15d", *(int*)&(record[offset]));
						}
						else {
							char buf[256];
							_itoa(*(int*)&(record[offset]), buf, 10);
							row_map[cd->col_name] = buf;
						}
					}
					else {
						char rep[258] = { 0 };
						strncpy(rep, record + offset, cd->col_len);
						rep[cd->col_len] = '\0';
						if (star == "*") {
							printf("%-20s", rep);
						}
						else {
						
							row_map[cd->col_name] = rep;
						
						}
					}
				}

				new_line = 1;

			}
			else if (!avg.empty()) {
				if (avg == cd->col_name) {
					if (record[j] != 0) {
						sum_val += *(int*)(record + offset);
						count_val += 1;
					}
				
				}
					
			}
			else if (!sum.empty()) {
				if (sum == cd->col_name) {
					if (record[j] != 0) {
						sum_val += *(int*)(record + offset);
					}
				}
			
			}
			else if (!count.empty()) {
				if (count == "*") {
						count_val += 1;
						break;
				}
				if (count == cd->col_name) {
					if (record[j] != 0) {
						count_val += 1;
					}
				}
			}

			offset += cd->col_len;
		}

		if (row_map.size() > 0) {
			for (auto& it : seq_cols) {
			
				printf("%-15s", row_map[it].c_str());
			
			}
		
		}

		if (new_line > 0) {
			printf("\n");
		}
	
	}

	if (!avg.empty()) {
		printf("avg: %f\n", count_val == 0 ? 0.0: (float)sum_val/(float)count_val);
			
	}
	else if (!sum.empty()) {
		printf("sum: %d\n", sum_val);
	
	}
	else if (!count.empty()) {
		printf("count: %d\n", count_val);
	}
	printf("|=====================================================================================|\n");
	free_vec(&tab.data);

	return 0;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
//	struct _stat file_stat;
	struct stat file_stat;

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
//		_fstat(_fileno(fhandle), &file_stat);
		fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			//fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}

tpd_list* load_tpd_list(FILE* f) {
	tpd_list* res = NULL;
	tpd_list temp;
	size_t size = 0;
	int temp_size = sizeof(tpd_list);
	//fread(&temp, sizeof(tpd_list), 1, f);
	fread(&temp, temp_size, 1, f);
	res = (tpd_list*)calloc(1, temp.list_size);
	*res = temp;
	fread(((char*)res) + sizeof(tpd_list), temp.list_size - sizeof(tpd_list), 1, f);

	
	return res;
}

	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}
cd_entry* get_cd_from_table(tpd_entry* table, int index) {
	if (index < table->num_columns) {
		return (cd_entry*)(((char*)table) + sizeof(tpd_entry) + index * sizeof(cd_entry));
	}
	
	return NULL;
}

int cal_record_size(tpd_entry* table) {
	int i = 0;
	cd_entry* cd = NULL;
	int count = table->num_columns;
	for (i = 0; i < table->num_columns; i++) {
		cd = get_cd_from_table(table, i);
		count += cd->col_len;
	}

	return (count % 4) == 0 ? count : (count + (4 - (count%4)));
}

void get_now(char* buff) {
	time_t t = time(NULL);
	struct tm* tmTime;
	tmTime = localtime(&t);
	char* format = "%Y%m%d%H%M%S";

	strftime(buff, 64, format, tmTime);
}

int backup(char* name) {
	FILE* handle = NULL;
	int i = 0;
	tpd_entry* start  = &(g_tpd_list->tpd_start);
	
	if (!_access(name, 0)) {
		return -1;
	}

	handle = fopen(name, "wbc");
	if (handle == NULL) {
		return -1;
	}

	save_tpd_list_stream(handle, g_tpd_list);
	for (i = 0; i < g_tpd_list->num_tables; i++) {
		tpd_entry* entry = start + i;
		Table tb;
		load_table(entry->table_name, &tb);
		save_table_stream(entry->table_name, &tb, handle);
	}
	fclose(handle);

	return 0;
}

int restore(char* name, int rf) {
	std::list<std::string> logs;
	std::string target = "BACKUP TO ";
	target = target + name;
	load_log(DB_LOG, logs);
	auto iter = std::find(logs.begin(), logs.end(), target);
	if (iter == logs.end()) {
		return -1;
	}

	iter++;
	if (rf) {
		if (iter == logs.end()) {
			logs.push_back("RF_START");
		}
		else {
			logs.insert(iter, "RF_START");
		}
		save_log(DB_LOG, logs);
	}
	else {	
		std::list<std::string> new_logs(logs.begin(), iter);
		rename_log();
		save_log(DB_LOG, new_logs);
	}

	FILE* handle = NULL;
	handle = fopen(name, "rbc");
	if (handle == NULL) {
		return -1;
	}

	int i = 0;
	tpd_list* tl = load_tpd_list(handle);
	if (tl == NULL) {
		return -1;
	}
	else {
		if (rf) {
			tl->db_flags = 1;
		}
		else {
			tl->db_flags = 0;
		}
		save_tpd_list("dbfile.bin", tl);
	}

	tpd_entry* start = &(tl->tpd_start);

	for (i = 0; i < tl->num_tables; i++) {
		Table temp;
		tpd_entry* entry = start + i;
		load_table_stream(entry->table_name, &temp, handle);
		save_table(entry->table_name, &temp);
	}
	fclose(handle);

	return 0;
}

int sem_backup(token_list* t_list) {
	token_list* cur = t_list;
	char* name = NULL;

	if (g_tpd_list->db_flags != 0) {
		return INVALID_STATEMENT;
	}
	if (cur == NULL || cur->tok_class != identifier) {
		return INVALID_STATEMENT;
	}
	name = cur->tok_string;
	
	return backup(name);
}

int sem_restore(token_list* t_list) {
	token_list* cur = t_list;
	char* name = NULL;
	int rf = 1;

	if (g_tpd_list->db_flags != 0) {
		return INVALID_STATEMENT;
	}

	if (cur == NULL || cur->tok_class != identifier) {
		return INVALID_STATEMENT;
	}

	name = cur->tok_string;
	cur = cur->next;
	if (cur != NULL && cur->tok_value != EOC) {
		if (cur == NULL || cur->tok_value != K_WITHOUT) {
			return INVALID_STATEMENT;
		}
		cur = cur->next;
		if (cur == NULL || cur->tok_value != K_RF) {
			return INVALID_STATEMENT;
		}
		rf = 0;
	}

	if (restore(name, rf) != 0) {
		return INVALID_STATEMENT;
	}
	initialize_tpd_list();
	return 0;
}

int sem_rollforward(token_list* t_list) {
	std::string end;
	auto cur = t_list;
	if (cur != NULL && cur->tok_value != EOC) {
		if (cur->tok_value != K_TO) {
			return INVALID_STATEMENT;
		}

		cur = cur->next;
		if (cur == NULL || cur->tok_value != T_INT) {
			return INVALID_STATEMENT;
		}
		
		end = cur->tok_string;
	}

	std::list<std::string> logs;
	std::string target = "RF_START";

	load_log(DB_LOG, logs);
	auto iter = std::find(logs.begin(), logs.end(), target);
	if (iter == logs.end()) {
		return -1;
	}
	g_tpd_list->db_flags = 0;

	save_tpd_list("dbfile.bin", g_tpd_list);

	std::list<std::string> new_logs(logs.begin(), iter);

	rename_log();
	save_log(DB_LOG, new_logs);
	iter = logs.erase(iter);
	char* temp_sql = g_sql;
	while (iter != logs.end() && (end.empty() || strncmp(iter->c_str(), end.c_str(), end.length()) <= 0)){
		char* s =  (char*)(iter->c_str() + 15);
		char* argv[] = { "db", s };
		main(2, argv);
		iter++;
	}
	g_sql = temp_sql;
	return 0;
}
