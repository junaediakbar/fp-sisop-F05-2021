#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>

const char auth_biasa[][1005] = {"[*]", "-u", "[*]", "-p", "[*]", ""};
const char auth_root[][1005] = {"[*]", ""};

const int SIZE_BUFFER = sizeof(char) * 500;
int login = 0;
int flag = 1;
char id[505] = {0};

void getServerInput(int fd, char *input)
{
    if (recv(fd, input, 500, 0) == 0)
    {
        printf("Server Dimatikan\n");
        exit(EXIT_SUCCESS);
    }
}

int delete_last_char(char *str, char c)
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
    chdir(".");
    int fd = *(int *)client_fd;
    char message[500] = {0};
    strcpy(message, id);
    send(fd, message, SIZE_BUFFER, 0);
    while (1)
    {
    	if (flag == 0)
            break;
        fgets(message, 1005, stdin);

        delete_last_char(message, '\n');
        send(fd, message, SIZE_BUFFER, 0);
    }
}

void *Output(void *client_fd)
{
    chdir(".");
    int fd = *(int *)client_fd;
    char message[500] = {0};

    while (1)
    {
        memset(message, 0, SIZE_BUFFER);
        getServerInput(fd, message);
        printf("%s", message);
        if(strcmp(message, "Login Gagal\n") == 0)
        {
        	flag = 0;
        	break;
        }
        fflush(stdout);
    }
}
bool cek_format(char **argv, int argc, const char format[][1005])
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

int main(int argc, char **argv)
{
    bool is_root = false;

    if (geteuid() == 0)
    {
        is_root = true;
    }
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
    int client_fd = fd;
    if (is_root)
    {
        if (cek_format(argv, argc, auth_root))
        {
          
            login = 1;
            printf("user root\n");
            char *tempid;
            strcpy(tempid,id);
            sprintf(id, "%sroot", tempid);
        }
    }
    else
    {
        if (cek_format(argv, argc, auth_biasa))
        {
            printf("user biasa\n");
            login = 1;
            sprintf(id, "%s %s", argv[2], argv[4]);
        }
    }
    if (login == 1)
    {
        pthread_t thread_id[2];
        pthread_create(&(thread_id[0]), NULL, &Input, (void *)&client_fd);
        pthread_create(&(thread_id[1]), NULL, &Output, (void *)&client_fd);

        pthread_join(thread_id[0], NULL);
        pthread_join(thread_id[1], NULL);
        close(client_fd);
    }
}
