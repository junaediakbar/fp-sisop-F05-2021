#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <dirent.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>

#define MAX_ROW 1024
#define MAX_COL 1024
#define COLC_LN 1
#define COLTYPE_LN 2
#define COLNAME_LN 3
#define WIC_LEN 20
#define DATA_BUFFER 300
#define CURR_DIR "."

/*COMMAND TEMPLATE*/

// char grant[][WIC_LEN];
// char use[][WIC_LEN];
// char create_tb[][WIC_LEN];
// char create_u[][WIC_LEN];
// char create_db[][WIC_LEN];
// char drop_col[][WIC_LEN];
// char drop_tb[][WIC_LEN];
// char drop_db[][WIC_LEN];
// char insert[][WIC_LEN];
// char update[][WIC_LEN];
// char delete[][WIC_LEN];
// char select[][WIC_LEN];

/*main db path*/
char db_main_path[MAX_COL];
const int SIZE_BUFFER = sizeof(char) * DATA_BUFFER;
char username[DATA_BUFFER], password[DATA_BUFFER];

char **get_2d_array_char(int r, int c)
{
    char **arr = (char **)malloc(r * sizeof(char *));
    for (int i = 0; i < r; i++)
    {
        arr[i] = (char *)malloc(c * sizeof(char));
    }
    return arr;
}

int tokenize(char *str, char **tokens, const char *delimiter, size_t tok_len)
{
    char *token;
    int i = 0;

    token = strtok(str, delimiter);

    while (token != NULL)
    {
        snprintf(tokens[i], tok_len, "%s", token);

        token = strtok(NULL, delimiter);
        i++;
    }

    free(token);

    return i;
}

int replace_word(char *result, const char *s, const char *old_w, const char *new_w, size_t res_len)
{
    if (result == NULL || s == NULL || old_w == NULL || new_w == NULL)
    {
        return -1;
    }

    char *final;
    int i, cnt = 0;
    int new_w_len = strlen(new_w);
    int old_w_len = strlen(old_w);

    for (i = 0; s[i] != '\0'; i++)
    {
        if (strstr(&s[i], old_w) == &s[i])
        {
            cnt++;

            i += old_w_len - 1;
        }
    }

    final = (char *)malloc(i + cnt * (new_w_len - old_w_len) + 1);

    i = 0;
    while (*s)
    {
        if (strstr(s, old_w) == s)
        {
            strcpy(&final[i], new_w);
            i += new_w_len;
            s += old_w_len;
        }
        else
            final[i++] = *s++;
    }

    final[i] = '\0';

    snprintf(result, res_len, "%s", final);

    free(final);

    return 0;
}

int is_cmd_valid(const char *cmd)
{
    if (cmd == NULL)
    {
        return -1;
    }

    char result[MAX_COL];
    char res_1[MAX_COL];
    char res_2[MAX_COL];

    snprintf(result, MAX_COL, "%s", cmd);

    if (strstr(cmd, ", ") != NULL)
    {
        const char old[] = ", ";
        const char new[] = ",";
        if (replace_word(res_1, cmd, old, new, MAX_COL) == -1)
        {
            return -1;
        }
        snprintf(result, MAX_COL, "%s", res_1);
    }

    char *open_bracket = strstr(res_1, "(");

    if (open_bracket != NULL)
    {
        puts("THERE IS OPEN BRACKET");
        if (strstr(open_bracket, " ") != NULL)
        {
            int begin = open_bracket - res_1;
            puts("THERE IS SPACE");
            const char old[] = " ";
            const char new[] = ",";
            if (replace_word(res_2, res_1 + begin, old, new, MAX_COL) == -1)
            {
                return -1;
            }
            snprintf(result + begin, MAX_COL - begin, "%s", res_2);
        }
    }

    puts(result);
}

int update_log(char *user, char *cmd)
{
    char line[MAX_COL];

    time_t raw_time;
    struct tm *time_info;
    time(&raw_time);
    time_info = localtime(&raw_time);

    FILE *file;
    file = fopen("./database.log", "a");

    if (file == NULL)
    {
        return -1;
    }

    int dd = time_info->tm_mday;
    int mm = time_info->tm_mon + 1;
    int yyyy = time_info->tm_year + 1900;
    int HH = time_info->tm_hour;
    int MM = time_info->tm_min;
    int SS = time_info->tm_sec;

    snprintf(line, sizeof(line), "%.4d-%.2d-%.2d %.2d:%.2d:%.2d:%s:%s\n",
             yyyy, mm, dd, HH, MM, SS, user, cmd);

    fprintf(file, "%s", line);

    fclose(file);
    return 0;
}

int read_file(char *path)
{
    char line[MAX_COL];

    FILE *file = fopen(path, "r");

    if (file == NULL)
    {
        return -1;
    }

    while (fgets(line, sizeof(line), file))
    {
        printf("%s", line);
    }

    fclose(file);

    return 0;
}

int get_line(char *result, char *path, int target_ln, size_t res_len)
{
    int i = 1;
    char line[MAX_COL];

    FILE *file = fopen(path, "r");

    if (file == NULL)
    {
        return -1;
    }

    while (fgets(line, sizeof(line), file))
    {
        if (i == target_ln)
        {
            snprintf(result, res_len, "%s", line);
            fclose(file);
            return 0;
        }

        i += 1;
    }

    fclose(file);
    return -1;
}

int get_colc(char *tb_path)
{
    int colc;
    char line[MAX_COL];
    char *endptr;

    FILE *file = fopen(tb_path, "r");

    if (file == NULL)
    {
        return -1;
    }

    /*get first line*/
    if (fgets(line, sizeof(line), file))
    {
        colc = (int)strtol(line, &endptr, 10);
    }

    fclose(file);

    return colc;
}

bool is_int(char *str)
{
    for (int i = 0; str[i] != '\0'; i++)
    {
        if (isdigit(str[i]) == false)
        {
            return false;
        }
    }
    return true;
}

int mk_dir(char *path)
{
    struct stat st = {0};

    /*if path not found*/
    if (stat(path, &st) == -1)
    {
        mkdir(path, 0777);
        return 0;
    }
    else
    {
        return -1;
    }
}

bool is_data_valid(char *tb_name, char **rowv, int colc)
{
    char type[MAX_COL] = "";

    if (get_line(type, tb_name, COLTYPE_LN, MAX_COL) == -1)
    {
        printf("ERROR: line not found\n");
        return false;
    }

    for (int i = 0; i < colc; i++)
    {
        /*check is integer valid*/
        if (type[i] == '1' && is_int(rowv[i]) == false)
        {
            printf("ERROR: invalid data\n");
            return false;
        }
    }

    return true;
}

int get_db_path(char *result, char *db_name, size_t res_len)
{
    if (result == NULL || db_name == NULL)
    {
        return -1;
    }

    snprintf(result, res_len, "%s/%s", db_main_path, db_name);

    return 0;
}

int get_tb_path(char *result, char *db_path, char *tb_name, size_t res_len)
{
    if (result == NULL || db_path == NULL || tb_name == NULL)
    {
        return -1;
    }

    snprintf(result, res_len, "%s/%s", db_path, tb_name);

    return 0;
}

bool is_dir_exist(char *path)
{
    DIR *dir = opendir(path);
    if (dir != NULL)
    {
        closedir(dir);
        return true;
    }

    return false;
}

bool is_file_exist(char *path)
{
    if (access(path, F_OK) == 0)
    {
        return true;
    }
    return false;
}

int del_last_chr(char *str, char c)
{
    char *cp = strchr(str, c);
    if (cp != NULL)
    {
        str[cp - str] = '\0';
        return 0;
    }
    return -1;
}

int get_coln(char *tb_path, char *col_name)
{
    char line[MAX_COL];

    if (get_line(line, tb_path, COLNAME_LN, MAX_COL) == -1)
    {
        printf("ERROR: line not found\n");
        return -1;
    }

    char **tokens = get_2d_array_char(MAX_ROW, MAX_COL);

    int ntok = tokenize(line, tokens, ",", MAX_COL);

    /*delete '\n'*/
    del_last_chr(tokens[ntok - 1], '\n');

    for (int i = 0; i < ntok; i++)
    {
        if (strcmp(tokens[i], col_name) == 0)
        {
            return (i + 1);
        }
    }

    free(tokens);

    return -1;
}

int update_line(char *result, char *tb_path, char *line, char *set_val, int target_coln, int colc, size_t res_len)
{
    char **tokens = get_2d_array_char(MAX_ROW, MAX_COL);
    char new_line[MAX_COL] = "";
    int ntok = tokenize(line, tokens, ",", MAX_COL);

    del_last_chr(tokens[ntok - 1], '\n');

    /*drop column*/
    if (set_val == NULL)
    {
        /*minus one val*/
        int comma = (ntok - 1) - 1;

        for (int i = 0; i < ntok; i++)
        {
            if (i != target_coln - 1)
            {
                strcat(new_line, tokens[i]);
                if (comma > 0)
                {
                    strcat(new_line, ",");
                    comma -= 1;
                }
            }
        }
    }
    /*update column*/
    else
    {
        /*replace value at target_coln*/
        snprintf(tokens[target_coln - 1], MAX_COL, "%s", set_val);

        if (is_data_valid(tb_path, tokens, colc) == false)
        {
            return -1;
        }

        for (int i = 0; i < ntok; i++)
        {
            strcat(new_line, tokens[i]);
            if (i < ntok - 1)
            {
                strcat(new_line, ",");
            }
        }
    }

    snprintf(result, res_len, "%s\n", new_line);

    free(tokens);

    return 0;
}

int remove_directory(const char *path)
{
    DIR *d = opendir(path);
    size_t path_len = strlen(path);
    int r = -1;

    if (d)
    {
        struct dirent *p;

        r = 0;
        while (!r && (p = readdir(d)))
        {
            int r2 = -1;
            char *buf;
            size_t len;

            /* Skip the names "." and ".." as we don't want to recurse on them. */
            if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, ".."))
                continue;

            len = path_len + strlen(p->d_name) + 2;
            buf = malloc(len);

            if (buf)
            {
                struct stat statbuf;

                snprintf(buf, len, "%s/%s", path, p->d_name);
                if (!stat(buf, &statbuf))
                {
                    if (S_ISDIR(statbuf.st_mode))
                        r2 = remove_directory(buf);
                    else
                        r2 = unlink(buf);
                }
                free(buf);
            }
            r = r2;
        }
        closedir(d);
    }

    if (!r)
        r = rmdir(path);

    return r;
}

bool is_where_valid(char *where_col, char *where_val)
{
    if ((where_col != NULL) != (where_val != NULL))
    {
        if (where_col == NULL)
        {
            printf("ERROR: where column cannot be empty\n");
            return false;
        }
        else if (where_val == NULL)
        {
            printf("ERROR: where value cannot be empty\n");
            return false;
        }
    }
    return true;
}

bool is_value_exist(char *line, char *where_val, int where_coln)
{
    char **tokens = get_2d_array_char(MAX_ROW, MAX_COL);
    int ntok = tokenize(line, tokens, ",", MAX_COL);

    del_last_chr(tokens[ntok - 1], '\n');

    int status = strcmp(tokens[where_coln - 1], where_val);

    free(tokens);

    return (status == 0);
}

void get_selected_col(char *result, char *line, int *s_colnv, int s_colc, size_t res_len)
{

    char new_line[MAX_COL] = "";
    char **tokens = get_2d_array_char(MAX_ROW, MAX_COL);

    int ntok = tokenize(line, tokens, ",", MAX_COL);

    del_last_chr(tokens[ntok - 1], '\n');

    /*minus one val*/
    int comma = s_colc - 1;

    for (int i = 0; i < s_colc; i++)
    {
        strcat(new_line, tokens[s_colnv[i] - 1]);
        if (comma > 0)
        {
            strcat(new_line, ",");
            comma -= 1;
        }
    }

    snprintf(result, res_len, "%s\n", new_line);

    free(tokens);

    return;
}

int init_db_path(char *result, char *db_name, size_t db_path_len)
{
    if (get_db_path(result, db_name, db_path_len) == -1)
    {
        return -1;
    }

    if (is_dir_exist(result) == false)
    {
        printf("ERROR: database does not exist\n");
        return -1;
    }
}

int add_db_access(char *result, char *line, char *new_db, int target_coln, size_t res_len)
{
    if (result == NULL || line == NULL || new_db == NULL)
    {
        return -1;
    }

    char **tokens = get_2d_array_char(MAX_ROW, MAX_COL), new_line[MAX_COL] = "", db_list[MAX_COL] = "";
    int ntok = tokenize(line, tokens, ",", MAX_COL);

    del_last_chr(tokens[ntok - 1], '\n');

    snprintf(db_list, MAX_COL, "%s", tokens[target_coln - 1]);

    char **db_tokens = get_2d_array_char(MAX_ROW, MAX_COL);
    int db_ntok = tokenize(tokens[target_coln - 1], db_tokens, ";", MAX_COL);

    /*check if new_db is already written*/
    for (int i = 0; i < db_ntok; i++)
    {
        if (strcmp(db_tokens[i], new_db) == 0)
        {
            free(tokens);
            free(db_tokens);
            printf("ERROR: database %s already exists\n", new_db);
            return -1;
        }
    }

    if (db_ntok == 0)
    {
        snprintf(tokens[target_coln - 1], MAX_COL, "%s", new_db);
    }
    else if (db_ntok > 0)
    {
        snprintf(tokens[target_coln - 1], 2048 ,"%s;%s", db_list, new_db);
    }

    for (int i = 0; i < ntok; i++)
    {
        strcat(new_line, tokens[i]);

        if (i < ntok - 1)
        {
            strcat(new_line, ",");
        }
    }

    snprintf(result, res_len, "%s\n", new_line);

    free(tokens);
    free(db_tokens);

    return 0;
}

int get_value_at_coln(char *result, char *tb_path, char *where_val, int where_coln, int target_coln, size_t res_len)
{
    if (result == NULL || tb_path == NULL || where_val == NULL)
    {
        return -1;
    }

    char line[MAX_COL], line_copy[MAX_COL];
    int row = 1;

    char **tokens = get_2d_array_char(MAX_ROW, MAX_COL);
    int ntok;

    FILE *file = fopen(tb_path, "r");

    if (file == NULL)
    {
        return -1;
    }

    while (fgets(line, sizeof(line), file))
    {
        snprintf(line_copy, MAX_COL, "%s", line);
        if (row > COLNAME_LN && is_value_exist(line_copy, where_val, where_coln))
        {
            ntok = tokenize(line, tokens, ",", MAX_COL);
            del_last_chr(tokens[ntok - 1], '\n');
            snprintf(result, res_len, "%s", tokens[target_coln - 1]);
            free(tokens);
            fclose(file);
            return 0;
        }

        row += 1;
    }

    fclose(file);
    free(tokens);

    return -1;
}

int is_user_registered(char *db_name, char *tb_name, char *username, char *password)
{
    char db_path[MAX_COL], tb_path[MAX_COL], result[MAX_COL];
    int target_coln;

    init_db_path(db_path, db_name, MAX_COL);

    if (get_tb_path(tb_path, db_path, tb_name, MAX_COL) == -1)
    {
        return -1;
    }

    if (is_file_exist(tb_path) == false)
    {
        printf("ERROR: table %s does not exist\n", tb_path);
        return -1;
    }

    target_coln = get_coln(tb_path, "username");

    if (target_coln == -1)
    {
        printf("ERROR: column not found\n");
        return -1;
    }

    char pw[MAX_COL];
    if (get_value_at_coln(pw, tb_path, username, 1, 2, MAX_COL) == 0)
    {
        if (strcmp(password, pw) == 0)
        {
            printf("INFO: login success\n");
            return 0;
        }
        else
        {
            printf("INFO: wrong password\n");
            return -1;
        }
    }

    printf("INFO: user not found\n");
    return -1;
}

int can_user_access_db(char *db_name, char *tb_name, char *username, char *db_access)
{
    char db_path[MAX_COL], tb_path[MAX_COL], result[MAX_COL];
    int target_coln;

    init_db_path(db_path, db_name, MAX_COL);

    if (get_tb_path(tb_path, db_path, tb_name, MAX_COL) == -1)
    {
        return -1;
    }

    if (is_file_exist(tb_path) == false)
    {
        printf("ERROR: table %s does not exist\n", tb_path);
        return -1;
    }

    target_coln = get_coln(tb_path, "access");

    if (target_coln == -1)
    {
        printf("ERROR: column not found\n");
        return -1;
    }

    char db_list[MAX_COL];

    if (get_value_at_coln(db_list, tb_path, username, 1, 3, MAX_COL) == -1)
    {
        return -1;
    }

    char **tokens = get_2d_array_char(MAX_COL, MAX_ROW);
    int ntok = tokenize(db_list, tokens, ";", MAX_COL);

    del_last_chr(tokens[ntok - 1], '\n');

    for (int i = 0; i < ntok; i++)
    {
        if (strcmp(tokens[i], db_access) == 0)
        {
            return 0;
        }
    }

    printf("ERROR: user does not have access to %s\n", db_access);

    free(tokens);

    return -1;
}

/*DUMP FUNCTIONS*/

int dump_table(char *tb_path, char *tb_name)
{
    char dump_path[MAX_COL];
    char line[MAX_COL], line_copy[MAX_COL], col_type[MAX_COL], col_name[MAX_COL], arg[MAX_COL] = "", val[MAX_COL], **tokens;
    int row = 1, ntok;

    snprintf(dump_path, MAX_COL, "./%s.backup", tb_name);

    int colc = get_colc(tb_path);
    if (colc == -1)
    {
        printf("ERROR: column not found\n");
        return -1;
    }

    FILE *file = fopen(tb_path, "r");

    if (file == NULL)
    {
        return -1;
    }

    FILE *file_dump = fopen(dump_path, "w");

    if (file_dump == NULL)
    {
        return -1;
    }

    /*drop*/
    fprintf(file_dump, "DROP TABLE %s;\n", tb_name);

    while (fgets(line, sizeof(line), file))
    {
        if (row == COLTYPE_LN)
        {
            snprintf(col_type, MAX_COL, "%s", line);
        }
        else if (row == COLNAME_LN)
        {
            snprintf(col_name, MAX_COL, "%s", line);

            tokens = get_2d_array_char(MAX_ROW, MAX_COL);
            ntok = tokenize(col_name, tokens, ",", MAX_COL);

            del_last_chr(tokens[ntok - 1], '\n');

            for (int i = 0; i < ntok; i++)
            {
                char data_type[MAX_COL];

                if (col_type[i] == '0')
                {
                    snprintf(data_type, MAX_COL, "%s", "string");
                }
                else if (col_type[i] == '1')
                {
                    snprintf(data_type, MAX_COL, "%s", "int");
                }

                strcat(arg, tokens[i]);
                strcat(arg, " ");
                strcat(arg, data_type);

                if (i < ntok - 1)
                {
                    strcat(arg, ", ");
                }
            }

            free(tokens);

            fprintf(file_dump, "CREATE TABLE %s (%s);\n", tb_name, arg);
        }
        else if (row > COLNAME_LN)
        {
            snprintf(line_copy, MAX_COL, "%s", line);

            tokens = get_2d_array_char(MAX_ROW, MAX_COL);
            ntok = tokenize(line_copy, tokens, ",", MAX_COL);

            del_last_chr(tokens[ntok - 1], '\n');

            snprintf(arg, MAX_COL, "%s", "");

            for (int i = 0; i < ntok; i++)
            {
                if (col_type[i] == '0')
                {
                    snprintf(val, MAX_COL, "'%s'", tokens[i]);
                }
                else if (col_type[i] == '1')
                {
                    snprintf(val, MAX_COL, "%s", tokens[i]);
                }

                strcat(arg, val);

                if (i < ntok - 1)
                {
                    strcat(arg, ", ");
                }
            }

            fprintf(file_dump, "INSERT INTO %s (%s);\n", tb_name, arg);

            free(tokens);
        }

        row += 1;
    }

    fclose(file);
    fclose(file_dump);

    return 0;
}

int dump(char *db_name)
{
    char db_path[MAX_COL], tb_path[MAX_COL];

    init_db_path(db_path, db_name, MAX_COL);

    DIR *dp;
    struct dirent *dir;

    dp = opendir(db_path);

    if (dp != NULL)
    {
        while ((dir = readdir(dp)))
        {

            if (strcmp(dir->d_name, ".") != 0 && strcmp(dir->d_name, "..") != 0)
            {
                if (get_tb_path(tb_path, db_path, dir->d_name, MAX_COL) == -1)
                {
                    return -1;
                }

                dump_table(tb_path, dir->d_name);
            }
        }

        (void)closedir(dp);
    }
    else
        perror("Couldn't open the directory");

    return 0;
}

int create_server_socket()
{
    struct sockaddr_in saddr;
    int fd, ret_val;
    int opt = 1;

    fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd == -1)
    {
        fprintf(stderr, "socket failed [%s]\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)))
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    printf("Created a socket with fd: %d\n", fd);

    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(7000);
    saddr.sin_addr.s_addr = INADDR_ANY;

    ret_val = bind(fd, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));
    if (ret_val != 0)
    {
        fprintf(stderr, "bind failed [%s]\n", strerror(errno));
        close(fd);
        exit(EXIT_FAILURE);
    }

    ret_val = listen(fd, 5);
    if (ret_val != 0)
    {
        fprintf(stderr, "listen failed [%s]\n", strerror(errno));
        close(fd);
        exit(EXIT_FAILURE);
    }
    return fd;
}

int getInput(int fd, char *prompt, char *storage)
{
    send(fd, prompt, SIZE_BUFFER, 0);
    int ret_val;
    ret_val = recv(fd, storage, DATA_BUFFER, 0);
    if (ret_val == 0)
        return ret_val;
    printf("Input: [%s]\n", storage);
    return ret_val;
}

void *routes(void *argv)
{
    int fd = *(int *)argv;
    char cmd[DATA_BUFFER];
    chdir(CURR_DIR);
    if (getInput(fd, "", cmd) != 0)
    {
        if (strcmp(cmd, "root") == 0)
        {
            send(fd, "Login root success\n", SIZE_BUFFER, 0);
        }
        else
        {
            char cmd_copy[MAX_COL];
            snprintf(cmd_copy, MAX_COL, "%s", cmd);
            char **tokens = get_2d_array_char(MAX_ROW, MAX_COL);
            int num_tokens = tokenize(cmd_copy, tokens, " ", MAX_COL);
            if (!is_user_registered("user", "user", tokens[0], tokens[1]))
            {
                strcpy(username, tokens[0]);
                strcpy(password, tokens[1]);
                send(fd, "Login user success\n", SIZE_BUFFER, 0);
            }
            else
                send(fd, "Login user failed\n", SIZE_BUFFER, 0);
        }
    }
    while (recv(fd, cmd, DATA_BUFFER, MSG_PEEK | MSG_DONTWAIT) != 0)
    {
        if (getInput(fd, "", cmd) != 0)
        {
            char *tempcmd;
            strcpy(tempcmd,cmd);
            sprintf(cmd, "%s\n", tempcmd);
            send(fd, cmd, SIZE_BUFFER, 0);
        }
        sleep(0.001);
    }
    printf("Close connection with fd: %d\n", fd);
    close(fd);
}

/*DDL & DML FUNCTIONS*/

int create_user(char *db_name, char *tb_name, char **rowv, int valc)
{
    char db_path[MAX_COL], tb_path[MAX_COL], line[MAX_COL] = "";
    int colc, target_coln;

    init_db_path(db_path, db_name, MAX_COL);

    if (get_tb_path(tb_path, db_path, tb_name, MAX_COL) == -1)
    {
        return -1;
    }

    if (is_file_exist(tb_path) == false)
    {
        printf("ERROR: table %s does not exist\n", tb_path);
        return -1;
    }

    colc = get_colc(tb_path);

    if (colc == -1)
    {
        printf("ERROR: unable to get table column count\n");
        return -1;
    }

    if (valc != colc)
    {
        printf("ERROR: total column do not match\n");
        return -1;
    }

    /*username*/
    target_coln = get_coln(tb_path, "username");

    if (target_coln == -1)
    {
        printf("ERROR: column not found\n");
        return -1;
    }

    char tmp[MAX_COL];
    if (get_value_at_coln(tmp, tb_path, rowv[0], 1, 1, MAX_COL) == 0)
    {
        printf("ERROR: user already registered\n");
        return -1;
    }

    FILE *file = fopen(tb_path, "a+");

    if (file == NULL)
    {
        return -1;
    }

    for (int i = 0; i < colc; i++)
    {
        strcat(line, rowv[i]);

        if (i < colc - 1)
        {
            strcat(line, ",");
        }
    }

    if (is_data_valid(tb_path, rowv, colc) == false)
    {
        fclose(file);
        return -1;
    }

    fprintf(file, "%s\n", line);
    fclose(file);

    return 0;
}

int create_database(char *db_name)
{
    char db_path[MAX_COL];

    if (get_db_path(db_path, db_name, MAX_COL) == -1)
    {
        return -1;
    }

    if (mk_dir(db_path) == -1)
    {
        printf("ERROR: %s already exists\n", db_path);
        return -1;
    }

    return 0;
}

int create_table(char *db_name, char *tb_name, char **colv, int colc)
{
    char col_name[MAX_COL] = "", type[MAX_COL] = "";
    char db_path[MAX_COL], tb_path[MAX_COL];

    init_db_path(db_path, db_name, MAX_COL);

    if (get_tb_path(tb_path, db_path, tb_name, MAX_COL) == -1)
    {
        return -1;
    }

    FILE *file = fopen(tb_path, "w+");

    if (file == NULL)
    {
        return -1;
    }

    int comma = colc / 2 - 1;

    /*column name at even index*/
    for (int i = 0; i < colc; i += 2)
    {
        strcat(col_name, colv[i]);

        /*
        if string, then 0
        if int, then 1
        */

        if (strcmp(colv[i + 1], "string") == 0)
        {
            strcat(type, "0");
        }
        else if (strcmp(colv[i + 1], "int") == 0)
        {
            strcat(type, "1");
        }
        else
        {
            printf("cdb: %d\n", colc);
            printf("ERROR: invalid data type\n");
            fclose(file);
            remove(tb_path);
            return -1;
        }

        if (comma > 0)
        {
            strcat(col_name, ",");
            comma -= 1;
        }
    }

    fprintf(file, "%d\n", colc / 2);
    fprintf(file, "%s\n", type);
    fprintf(file, "%s\n", col_name);

    fclose(file);

    return 0;
}

int insert(char *db_name, char *tb_name, char **rowv, int valc)
{
    char db_path[MAX_COL], tb_path[MAX_COL], line[MAX_COL] = "";
    int colc;

    init_db_path(db_path, db_name, MAX_COL);

    if (get_tb_path(tb_path, db_path, tb_name, MAX_COL) == -1)
    {
        return -1;
    }

    if (is_file_exist(tb_path) == false)
    {
        printf("ERROR: table %s does not exist\n", tb_path);
        return -1;
    }

    colc = get_colc(tb_path);

    if (colc == -1)
    {
        printf("ERROR: unable to get table column count\n");
        return -1;
    }

    if (valc != colc)
    {
        printf("ERROR: total column do not match\n");
        return -1;
    }

    FILE *file = fopen(tb_path, "a+");

    if (file == NULL)
    {
        return -1;
    }

    for (int i = 0; i < colc; i++)
    {
        strcat(line, rowv[i]);

        if (i < colc - 1)
        {
            strcat(line, ",");
        }
    }

    if (is_data_valid(tb_path, rowv, colc) == false)
    {
        fclose(file);
        return -1;
    }

    fprintf(file, "%s\n", line);
    fclose(file);

    return 0;
}

int drop(char *db_name, char *tb_name, char *col_name, char mode)
{
    char line[MAX_COL];
    int colc, target_coln, row = 1;

    char db_path[MAX_COL], tb_path[MAX_COL], tb_path_copy[MAX_COL];

    init_db_path(db_path, db_name, MAX_COL);

    switch (mode)
    {
    case 'd':
        remove_directory(db_path);
        printf("INFO: database %s deleted successfully\n", db_name);
        break;
    case 't':
        if (get_tb_path(tb_path, db_path, tb_name, MAX_COL) == -1)
        {
            return -1;
        }

        if (is_file_exist(tb_path) == false)
        {
            printf("ERROR: table %s does not exist\n", tb_path);
            return -1;
        }

        if (remove(tb_path) == 0)
        {
            printf("INFO: table %s deleted successfully\n", tb_name);
        }
        else
        {
            printf("ERROR: unable to delete table\n");
            return -1;
        }
        break;
    case 'c':
        if (get_tb_path(tb_path, db_path, tb_name, MAX_COL) == -1)
        {
            return -1;
        }

        if (is_file_exist(tb_path) == false)
        {
            printf("ERROR: table %s does not exist\n", tb_path);
            return -1;
        }

        colc = get_colc(tb_path);

        if (colc == -1)
        {
            printf("ERROR: unable to get table column count\n");
            return -1;
        }

        if (colc == 1)
        {
            printf("ERROR: table must at least have 1 column\n");
            return -1;
        }

        target_coln = get_coln(tb_path, col_name);

        if (target_coln == -1)
        {
            printf("ERROR: column not found\n");
            return -1;
        }

        FILE *file = fopen(tb_path, "r");

        if (file == NULL)
        {
            return -1;
        }

        snprintf(tb_path_copy, 10000, "%s(copy)", tb_path);

        FILE *file_copy = fopen(tb_path_copy, "w");

        if (file_copy == NULL)
        {
            return -1;
        }

        while (fgets(line, sizeof(line), file))
        {
            /*TOTAL COLUMN*/
            if (row == COLC_LN)
            {
                snprintf(line, sizeof(line), "%d\n", colc - 1);
            }
            /*COLUMN TYPE*/
            else if (row == COLTYPE_LN)
            {
                // printf("%u %lu %lu\n", target_coln - 1, strlen(line) - 1, strlen(line) - target_coln);
                memmove(line + target_coln - 1, line + target_coln, strlen(line) - target_coln + 1);
            }
            /*DATA*/
            else if (row > COLTYPE_LN)
            {
                if (update_line(line, tb_path, line, NULL, target_coln, colc, MAX_COL) == -1)
                {
                    fclose(file);
                    fclose(file_copy);
                    remove(tb_path_copy);
                    return -1;
                }
            }

            fprintf(file_copy, "%s", line);

            // printf("%s", line);

            row += 1;
        }

        fclose(file);
        remove(tb_path);
        fclose(file_copy);
        rename(tb_path_copy, tb_path);

        break;
    }

    return 0;
}

int update(char *db_name, char *tb_name, char *set_col, char *set_val, char *where_col, char *where_val)
{
    char line[MAX_COL], line_copy[MAX_COL];
    int set_coln, where_coln, row = 1;

    char db_path[MAX_COL], tb_path[MAX_COL], tb_path_copy[MAX_COL];

    init_db_path(db_path, db_name, MAX_COL);

    if (get_tb_path(tb_path, db_path, tb_name, MAX_COL) == -1)
    {
        return -1;
    }

    if (is_file_exist(tb_path) == false)
    {
        printf("ERROR: table %s does not exist\n", tb_path);
        return -1;
    }

    int colc = get_colc(tb_path);

    if (colc == -1)
    {
        printf("ERROR: unable to get table column count\n");
        return -1;
    }

    if (set_col == NULL)
    {
        printf("ERROR: set column cannot be empty\n");
        return -1;
    }
    else if (set_val == NULL)
    {
        printf("ERROR: set value cannot be empty\n");
        return -1;
    }

    set_coln = get_coln(tb_path, set_col);

    if (set_coln == -1)
    {
        printf("ERROR: column not found\n");
        return -1;
    }

    /*check WHERE validity*/
    if (is_where_valid(where_col, where_val) == false)
    {
        return -1;
    }

    FILE *file = fopen(tb_path, "r");

    if (file == NULL)
    {
        return -1;
    }

    FILE *file_copy = fopen(tb_path_copy, "w");

    if (file_copy == NULL)
    {
        return -1;
    }

    if (where_col != NULL && where_val != NULL)
    {
        where_coln = get_coln(tb_path, where_col);

        if (where_coln == -1)
        {
            printf("ERROR: where column not found\n");
            return -1;
        }

        while (fgets(line, sizeof(line), file))
        {

            snprintf(line_copy, sizeof(line_copy), "%s", line);
            if ((row > COLNAME_LN) && is_value_exist(line_copy, where_val, where_coln))
            {
                if (update_line(line, tb_path, line, set_val, set_coln, colc, MAX_COL) == -1)
                {
                    fclose(file);
                    fclose(file_copy);
                    remove(tb_path_copy);
                    return -1;
                }
            }

            fprintf(file_copy, "%s", line);

            row += 1;
        }
    }
    else if (where_col == NULL && where_val == NULL)
    {
        while (fgets(line, sizeof(line), file))
        {
            if (row > COLNAME_LN)
            {
                if (update_line(line, tb_path, line, set_val, set_coln, colc, MAX_COL) == -1)
                {
                    fclose(file);
                    fclose(file_copy);
                    remove(tb_path_copy);
                    return -1;
                }
            }

            fprintf(file_copy, "%s", line);

            row += 1;
        }
    }

    fclose(file);
    remove(tb_path);
    fclose(file_copy);
    rename(tb_path_copy, tb_path);

    return 0;
}

int delete (char *db_name, char *tb_name, char *where_col, char *where_val)
{
    char line[MAX_COL], line_copy[MAX_COL];
    char *line_ptr;
    int where_coln, row = 1;

    char db_path[MAX_COL], tb_path[MAX_COL], tb_path_copy[MAX_COL];

    init_db_path(db_path, db_name, MAX_COL);

    if (get_tb_path(tb_path, db_path, tb_name, MAX_COL) == -1)
    {
        return -1;
    }

    if (is_file_exist(tb_path) == false)
    {
        printf("ERROR: table %s does not exist\n", tb_path);
        return -1;
    }

    /*check WHERE validity*/
    if (is_where_valid(where_col, where_val) == false)
    {
        return -1;
    }

    FILE *file = fopen(tb_path, "r");

    if (file == NULL)
    {
        return -1;
    }

    snprintf(tb_path_copy, 10000, "%s(copy)", tb_path);

    FILE *file_copy = fopen(tb_path_copy, "w");

    if (file_copy == NULL)
    {
        return -1;
    }

    /*WHERE statement exists*/
    if (where_col != NULL && where_val != NULL)
    {
        where_coln = get_coln(tb_path, where_col);

        if (where_coln == -1)
        {
            printf("ERROR: where column not found\n");
            return -1;
        }

        while (fgets(line, sizeof(line), file))
        {
            snprintf(line_copy, sizeof(line_copy), "%s", line);
            if (row > COLNAME_LN && is_value_exist(line_copy, where_val, where_coln))
            {
                continue;
            }

            fprintf(file_copy, "%s", line);

            row += 1;
        }
    }
    /*WHERE statement does not exist*/
    else if (where_col == NULL && where_val == NULL)
    {
        while (fgets(line, sizeof(line), file))
        {
            if (row > COLNAME_LN)
            {
                break;
            }

            fprintf(file_copy, "%s", line);

            // printf("%s", line);

            row += 1;
        }
    }

    fclose(file);
    remove(tb_path);
    fclose(file_copy);
    rename(tb_path_copy, tb_path);

    return 0;
}

int select_row(char *db_name, char *tb_name, char **s_colv, char *where_col, char *where_val, int s_colc)
{
    char line[MAX_COL], line_copy[MAX_COL], result[MAX_COL];
    int s_colnv[MAX_COL];
    int where_coln, row = 1;
    char db_path[MAX_COL], tb_path[MAX_COL];

    init_db_path(db_path, db_name, MAX_COL);

    if (get_tb_path(tb_path, db_path, tb_name, MAX_COL) == -1)
    {
        return -1;
    }

    if (is_file_exist(tb_path) == false)
    {
        printf("ERROR: table %s does not exist\n", tb_path);
        return -1;
    }

    int colc = get_colc(tb_path);

    if (colc == -1)
    {
        printf("ERROR: unable to get table column count\n");
        return -1;
    }

    /*check WHERE validity*/
    if (is_where_valid(where_col, where_val) == false)
    {
        return -1;
    }

    if (s_colv == NULL)
    {
        return -1;
    }

    for (int i = 0; i < s_colc; i++)
    {
        s_colnv[i] = get_coln(tb_path, s_colv[i]);
    }

    FILE *file = fopen(tb_path, "r");

    if (file == NULL)
    {
        return -1;
    }

    /*WHERE statement exists*/
    if (where_col != NULL && where_val != NULL)
    {
        where_coln = get_coln(tb_path, where_col);

        if (where_coln == -1)
        {
            printf("ERROR: where column not found\n");
            return -1;
        }

        while (fgets(line, sizeof(line), file))
        {
            if (row == COLNAME_LN)
            {
                get_selected_col(result, line, s_colnv, s_colc, MAX_COL);
                printf("%s", result);
            }

            snprintf(line_copy, sizeof(line_copy), "%s", line);
            if (row > COLNAME_LN && is_value_exist(line_copy, where_val, where_coln))
            {
                get_selected_col(result, line, s_colnv, s_colc, MAX_COL);
                printf("%s", result);
            }

            row += 1;
        }
    }
    /*WHERE statement does not exist*/
    else if (where_col == NULL && where_val == NULL)
    {
        while (fgets(line, sizeof(line), file))
        {
            if (row >= COLNAME_LN)
            {
                get_selected_col(result, line, s_colnv, s_colc, MAX_COL);
                printf("%s", result);
            }

            row += 1;
        }
    }

    fclose(file);

    return 0;
}

int grant_permission(char *db_name, char *tb_name, char *target_col, char *where_col, char *where_val, char *new_db)
{
    char line[MAX_COL], line_copy[MAX_COL];
    int where_coln, target_coln, row = 1;
    char db_path[MAX_COL], tb_path[MAX_COL], tb_path_copy[MAX_COL];

    init_db_path(db_path, db_name, MAX_COL);

    if (get_tb_path(tb_path, db_path, tb_name, MAX_COL) == -1)
    {
        return -1;
    }

    if (is_file_exist(tb_path) == false)
    {
        printf("ERROR: table %s does not exist\n", tb_path);
        return -1;
    }

    /*check WHERE validity*/
    if (is_where_valid(where_col, where_val) == false)
    {
        return -1;
    }

    if (target_col == NULL)
    {
        return -1;
    }

    target_coln = get_coln(tb_path, target_col);

    if (target_coln == -1)
    {
        printf("ERROR: target column not found\n");
        return -1;
    }

    if (where_col == NULL)
    {
        return -1;
    }

    where_coln = get_coln(tb_path, where_col);

    if (where_coln == -1)
    {
        printf("ERROR: where column not found\n");
        return -1;
    }

    FILE *file = fopen(tb_path, "r");

    if (file == NULL)
    {
        return -1;
    }

    snprintf(tb_path_copy, 10000, "%s(copy)", tb_path);

    FILE *file_copy = fopen(tb_path_copy, "w");

    if (file_copy == NULL)
    {
        return -1;
    }

    if (where_col != NULL && where_val != NULL)
    {
        while (fgets(line, sizeof(line), file))
        {
            snprintf(line_copy, sizeof(line_copy), "%s", line);
            if (row > COLNAME_LN && is_value_exist(line_copy, where_val, where_coln))
            {
                /*recopy*/
                snprintf(line_copy, sizeof(line_copy), "%s", line);
                if (add_db_access(line, line_copy, new_db, target_coln, MAX_COL) == -1)
                {
                    fclose(file);
                    fclose(file_copy);
                    remove(tb_path_copy);
                    return -1;
                }
            }

            fprintf(file_copy, "%s", line);

            row += 1;
        }
    }

    fclose(file);
    remove(tb_path);
    fclose(file_copy);
    rename(tb_path_copy, tb_path);

    return 0;
}

void init()
{
    /*create "databases" directory*/
    snprintf(db_main_path, sizeof(db_main_path), "%s", "./databases");

    if (mk_dir(db_main_path) == -1)
    {
        printf("ERROR: %s already exists\n", db_main_path);
    }

    char user_db[] = "user";
    char **colv = get_2d_array_char(6, MAX_COL);
    snprintf(colv[0], MAX_COL, "%s", "username");
    snprintf(colv[1], MAX_COL, "%s", "string");
    snprintf(colv[2], MAX_COL, "%s", "password");
    snprintf(colv[3], MAX_COL, "%s", "string");
    snprintf(colv[4], MAX_COL, "%s", "access");
    snprintf(colv[5], MAX_COL, "%s", "string");

    create_database(user_db);
    create_table(user_db, user_db, colv, 6);

    /*test create user*/
    snprintf(colv[0], MAX_COL, "%s", "daniel");
    snprintf(colv[1], MAX_COL, "%s", "123");
    snprintf(colv[2], MAX_COL, "%s", "");
    create_user("user", "user", colv, 3);
    /*test create user*/
    snprintf(colv[0], MAX_COL, "%s", "nanda");
    snprintf(colv[1], MAX_COL, "%s", "456");
    snprintf(colv[2], MAX_COL, "%s", "");
    create_user("user", "user", colv, 3);

    grant_permission("user", "user", "access", "username", "daniel", "db2");
    grant_permission("user", "user", "access", "username", "daniel", "db3");

    return;
}

int main(int argc, char **argv)
{

    init();
    socklen_t addrlen;
    struct sockaddr_in new_addr;
    pthread_t tid;
    char buf[DATA_BUFFER];
    int new_fd, ret_val;
    int server_fd = create_server_socket();

    while (1)
    {
        // accept setiap ada client yang menghubungkan dirinya ke server
        new_fd = accept(server_fd, (struct sockaddr *)&new_addr, &addrlen);
        if (new_fd >= 0)
        {
            printf("Accepted a new connection with fd: %d\n", new_fd);
            pthread_create(&tid, NULL, &routes, (void *)&new_fd);
            //	    		routes(new_fd);
        }
        else
        {
            fprintf(stderr, "Accept failed [%s]\n", strerror(errno));
        }
    }
    create_database("db1");
    // create_database("db2");

    /*create table*/
    char **tmp = get_2d_array_char(MAX_ROW, MAX_COL);
    strcpy(tmp[0], "c1");
    strcpy(tmp[1], "int");
    strcpy(tmp[2], "c2");
    strcpy(tmp[3], "string");
    strcpy(tmp[4], "c3");
    strcpy(tmp[5], "int");
    strcpy(tmp[6], "c4");
    strcpy(tmp[7], "string");

    create_table("db1", "t1", tmp, 8);

    /*insert 1*/
    char **ins = get_2d_array_char(4, MAX_COL);
    strcpy(ins[0], "123");
    strcpy(ins[1], "str1");
    strcpy(ins[2], "456");
    strcpy(ins[3], "str2");
    insert("db1", "t1", ins, 4);

    /*insert 2*/
    strcpy(ins[0], "111");
    strcpy(ins[1], "str3");
    strcpy(ins[2], "222");
    strcpy(ins[3], "str4");
    insert("db1", "t1", ins, 4);

    /*before*/
    read_file("./databases/db1/t1");

    /*drop col*/
    // drop("db1", "t1", "c1", 'c');
    // drop("db1", "t1", "c2", 'c');
    // drop("db1", "t1", "c3", 'c');
    // drop("db1", "t1", "c4", 'c');

    /*drop table*/
    // drop("db1", "t1", "c4", 't');

    // /*drop db*/
    // drop("db1", "t1", NULL, 'd');

    /*update column*/
    // update("db1", "t1", "c1", "123", NULL, NULL);
    // update("db1", "t1", "c2", "123", NULL, NULL);
    // update("db1", "t1", "c3", "123", NULL, NULL);
    // update("db1", "t1", "c4", "123", NULL, NULL);

    /*update column where*/
    // update("db1", "t1", "c4", "string2_new", "c1", "111");
    // update("db1", "t1", "c2", "string1_new", "c2", "str1");

    /*delete*/
    // delete ("db1", "t1", NULL, NULL);

    /*delete where*/
    // delete ("db1", "t1", "c2", "str1");

    // char **s_colv = get_2d_array_char(MAX_COL, MAX_ROW);
    // strcpy(s_colv[0], "c1");
    // strcpy(s_colv[1], "c2");
    // strcpy(s_colv[2], "c4");

    // /*select*/
    // select_row("db1", "t1", s_colv, NULL, NULL, 3);

    /*select where*/
    // select_row("db1", "t1", s_colv, "c4", "str4", 3);

    /*update log*/
    // update_log("daniel", "SELECT * FROM tb1");

    /*getval*/
    // char s[MAX_COL];
    // get_value_at_coln(s, "./databases/db1/t1", "111", 1, 1, MAX_COL);
    // printf("result %s\n", s);

    /*auth*/
    // printf("%d\n", is_user_registered("user", "user", "daniel", "123"));
    // printf("%d\n", can_user_access_db("user", "user", "daniel", "db1"));

    /*dump*/
    // dump("db1");

    // is_cmd_valid("INSERT INTO table1 (‘value1’, 2, ‘value3’, 4);");

    /*after*/
    read_file("./databases/db1/t1");
}