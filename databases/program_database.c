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

char db_main_path[1024];
const int SIZE_BUFFER = sizeof(char) * 300;
char username[300], password[300];


int main(int argc, char **argv)
{
    socklen_t addrlen;
    struct sockaddr_in new_addr;
    pthread_t thread_id;
    char buf[300];
    struct sockaddr_in saddr;
    int server_fd, ret_val, new_fd;
    int opt = 1;

    server_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (server_fd == -1)
    {
        fprintf(stderr, "socket failed [%s]\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)))
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    printf("Created a socket with fd: %d\n", server_fd);

    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(7000);
    saddr.sin_addr.s_addr = INADDR_ANY;

    ret_val = bind(server_fd, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));
    if (ret_val != 0)
    {
        fprintf(stderr, "bind failed [%s]\n", strerror(errno));
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    ret_val = listen(server_fd, 5);
    
    if (ret_val != 0)
    {
        fprintf(stderr, "listen failed [%s]\n", strerror(errno));
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    while (1)
    {
        new_fd = accept(server_fd, (struct sockaddr *)&new_addr, &addrlen);
        if (new_fd >= 0)
        {
            printf("Accepted Connection with fd: %d\n", new_fd);
            
        }
        else
        {
            fprintf(stderr, "Accept failed [%s]\n", strerror(errno));
        }
    }
}
