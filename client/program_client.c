#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#define AUTH_ARGC_NORMAL 5
#define AUTH_ARGC_ROOT 1
#define MAX_ROW 1024
#define MAX_COL 1024
#define DATA_BUFFER 1024
#define CURR_DIR "."

const char add_user[][MAX_COL] = {"CREATE", "USER", "[*]", "IDENTIFIED", "BY", "[*]", ""};
const char auth_normal[][MAX_COL] = {"[*]", "-u", "[*]", "-p", "[*]", ""};
const char auth_root[][MAX_COL] = {"[*]", ""};
const int SIZE_BUFFER = sizeof(char) * DATA_BUFFER;
int login = 0;
int flag = 1;
char id[DATA_BUFFER] = {0};

void getServerInput(int fd, char *input)
{
    if (recv(fd, input, DATA_BUFFER, 0) == 0)
    {
        printf("Server shutdown\n");
        exit(EXIT_SUCCESS);
    }
}

int create_client_socket()
{
    struct sockaddr_in saddr;
    int fd, ret_val;
    int opt = 1;
    struct hostent *local_host;

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
    local_host = gethostbyname("127.0.0.1");
    saddr.sin_addr = *((struct in_addr *)local_host->h_addr);

    ret_val = connect(fd, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));
    if (ret_val == -1)
    {
        fprintf(stderr, "connect failed [%s]\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
    return fd;
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

void *Input(void *client_fd)
{
    chdir(CURR_DIR);
    int fd = *(int *)client_fd;
    char message[DATA_BUFFER] = {0};
    strcpy(message, id);
    send(fd, message, SIZE_BUFFER, 0);
    while (1)
    {
    	if (flag == 0)
            break;
        /*fgets safer*/
        fgets(message, MAX_COL, stdin);

        del_last_chr(message, '\n');

        
        send(fd, message, SIZE_BUFFER, 0);
    }
}

void *Output(void *client_fd)
{
    chdir(CURR_DIR);
    int fd = *(int *)client_fd;
    char message[DATA_BUFFER] = {0};

    while (1)
    {
        memset(message, 0, SIZE_BUFFER);
        getServerInput(fd, message);
        printf("%s", message);
        if(strcmp(message, "Login user failed\n") == 0)
        {
        	flag = 0;
        	break;
        }
        fflush(stdout);
    }
}

char **get_2d_array_char(int r, int c)
{
    char **argv = (char **)malloc(r * sizeof(char *));
    for (int i = 0; i < r; i++)
    {
        argv[i] = (char *)malloc(c * sizeof(char));
    }
    return argv;
}

int tokenize(char *str, char **tokens, const char *delimiter)
{
    char *token;
    int i = 0;

    token = strtok(str, delimiter);

    while (token != NULL)
    {
        strcpy(tokens[i], token);
        // printf("%s\n", token);
        token = strtok(NULL, delimiter);
        i++;
    }

    return i;
}

bool is_format_valid(char **argv, int argc, const char format[][MAX_COL])
{
    int fm_argc = 0;

    for (int i = 0; strcmp(format[i], "") != 0; i++)
    {
        printf("%s %s\n", format[i], argv[i]);

        if (strcmp(format[i], "[*]") != 0 && strcmp(format[i], argv[i]) != 0)
        {
            return false;
        }
        fm_argc += 1;
    }

    if (argc != fm_argc)
    {
        return false;
    }

    return true;
}

void auth(int argc, char **argv, bool is_root)
{
    int client_fd = create_client_socket();
    if (is_root)
    {
        if (is_format_valid(argv, argc, auth_root))
        {
            printf("root user\n");
            login = 1;
            char *tempid;
            strcpy(tempid,id);
            sprintf(id, "%sroot", tempid);
            //            char cmd[MAX_COL];
            //            char **tokens = get_2d_array_char(MAX_ROW, MAX_COL);

            //            scanf("%[^;]s", cmd);

            //            printf("%s\n", cmd);

            //            int num_tokens = tokenize(cmd, tokens, " ");

            //            printf("num tokens: %d\n", num_tokens);

            //            if (is_format_valid(tokens, num_tokens, add_user))
            //            {
            //                printf("user: %s; pass: %s\n", tokens[2], tokens[5]);
            //            }
        }
    }
    else
    {
        if (is_format_valid(argv, argc, auth_normal))
        {
            printf("normal user\n");
            login = 1;
            sprintf(id, "%s %s", argv[2], argv[4]);
        }
    }
    if (login == 1)
    {
        pthread_t tid[2];
        pthread_create(&(tid[0]), NULL, &Input, (void *)&client_fd);
        pthread_create(&(tid[1]), NULL, &Output, (void *)&client_fd);

        pthread_join(tid[0], NULL);
        pthread_join(tid[1], NULL);
        close(client_fd);
    }
    return;
}

int main(int argc, char **argv)
{
    bool is_root = false;

    /*check root*/
    if (geteuid() == 0)
    {
        is_root = true;
    }

    auth(argc, argv, is_root);
}
