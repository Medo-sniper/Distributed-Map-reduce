#include <netdb.h> 
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h> 
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/wait.h>
#include <signal.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h> // non-blocking sockets
#include <stdbool.h>

#define TRUE 1
#define FALSE 0
#define PORT 8080 
#define SA struct sockaddr 
#define BUFSIZE 1024

int ret;
int sockfd, connfd; 
struct sockaddr_in servaddr, cli; 
struct hostent *master;
char* hostname;
	
int tried, connected;
int c;

// threads
pthread_t connectthread;
pthread_t getworkloadthread;
pthread_t dotaskthread;
pthread_t sendresultthread;

int output_fd;
ssize_t ret_out;
char buf[BUFSIZE];
char word[20];
int count;
int task;
int n;

typedef struct MSG {
    char mbuf[BUFSIZE];
    char mword[20];
    int mcount;
}MSG;

struct MSG msg;


void error(char *msg) {
	perror(msg);
	exit(1);
}


void* connectto(void* argv) {
	// socket create and varification 
	sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0); 
	if (sockfd == -1) { 
		printf("Socket creation failed...\n"); 
		exit(0); 
	} else {
		printf("Socket successfully created..\n"); 
	}
	
   	// gethostbyname: get the server's DNS entry
    master = gethostbyname(hostname);
	if (master == NULL) {
		fprintf(stderr,"ERROR, no such host as %s\n", hostname);
        exit(0);
    }
    
	bzero(&servaddr, sizeof(servaddr)); 
	// assign IP, PORT 
	servaddr.sin_family = AF_INET; 
	//servaddr.sin_addr.s_addr = inet_addr("127.0.0.1"); 
	bcopy((char *) master->h_addr, (char *) &servaddr.sin_addr.s_addr,master->h_length);
	servaddr.sin_port = htons(PORT); 

	tried = 0;
	connected = 0;
	while (connected == 0 && tried <= 10) {
		// connect the client socket to server socket 
		if (connect(sockfd, (SA*)&servaddr, sizeof(servaddr)) < 0) { 
			printf("Connection with the server failed...\n");
			tried++; 
			sleep(1); 
		} 
		else {
			connected = 1;
			c = TRUE;
			printf("Connected to the server..\n");
		}
	}
}


void* get_work_load (void* argv) {
	// Get Data
	int c = FALSE;
    output_fd = open("file1.txt",O_CREAT | O_WRONLY , S_IWUSR | S_IWGRP | S_IWOTH);
    if(output_fd == -1) {
    	perror("open2");
    }
    
    bzero((char *) &msg,sizeof(MSG));
    while (c == FALSE) {
    	bzero(buf,BUFSIZE);
    	n = read(sockfd,&msg,sizeof(MSG));
    	if (n < 0)
    	{
    		if(errno == EAGAIN || errno == EWOULDBLOCK)
    		{
    			continue;
       		}
    	} else if (n > 0 && (strcmp(msg.mword," ") == 0)) {
    		strcpy(buf,msg.mbuf);
    		printf("%s\n",buf);
	    	ret_out = write(output_fd,&msg.mbuf,BUFSIZE);
    		c = TRUE;
    	} else {
    		break;
    	}
    }
    
    // Get Task   
    bzero((char *) &msg,sizeof(MSG));

    c = FALSE;
    while (c == FALSE) {
    	n = read(sockfd,&msg,sizeof(MSG));
    	if (n < 0)
    	{
    		if(errno == EAGAIN || errno == EWOULDBLOCK)
    		{
    			continue;
       		}
    	} else if (n > 0 && (strcmp(msg.mword," ") != 0)) {
    		strcpy(word,msg.mword);
    		count = msg.mcount;
    		task = 0;
    		printf("Task recieved: count word(%s) Occurance! \n\n",word);
    		c = TRUE;
    	} else {
    		break;
    	}
    }     
} 



void* do_task(void* argv) {
	int bufsize = strlen(buf);
	printf("Buf size: %d \n",bufsize);
	int wordsize = strlen(word);
	char temp[wordsize];
	printf("The word size: %d \n",wordsize);
	
	for(int i = 0; i < bufsize; i++) {
			strncpy(temp, &buf[i], wordsize);
			if(strcmp(word,temp) == 0) {
				count += 1;
				i += (wordsize -1);
			}
	}
	
	bzero((char *)&msg,sizeof(MSG));
	strcpy(msg.mbuf," ");
	strcpy(msg.mword,word);
	msg.mcount = count;
	printf("The word (%s) found %d times! \n\n", word, count);
	task = 1;
}


void* send_result(void* argv) {
	c = FALSE;
    while (c == FALSE) { 
		n = write(sockfd, &msg, sizeof(MSG));
		if (n < 0) {
    		if(errno == EAGAIN || errno == EWOULDBLOCK)
    		{
    			continue;
       		}
		} else {
			c = TRUE;
			printf("Result sent to Master! \n");
		}
	}
}


int main(int argc, char** argv)  { 

    hostname = argv[1];
    ret = pthread_create(&connectthread, NULL, connectto, NULL);
    pthread_join(connectthread, NULL);
    printf("connectingthread thread returns %d\n",ret);
    sleep(3);
    
    ret = pthread_create(&getworkloadthread, NULL, get_work_load, NULL);
    pthread_join(getworkloadthread, NULL);
    printf("getworkload thread returns %d\n",ret);
    sleep(3);
    
    ret = pthread_create(&dotaskthread, NULL, do_task, NULL);
    pthread_join(dotaskthread, NULL);
    printf("dotaskthread thread returns %d\n",ret);  
    sleep(3);
    while(c == FALSE) {
    	;
    }
    
    ret = pthread_create(&sendresultthread, NULL, send_result, NULL);
    pthread_join(sendresultthread, NULL);
    printf("sendresultthread thread returns %d\n",ret);  
    sleep(3);
    
	printf("Closing the Connection...\n\n"); 
	close(sockfd);
	
	return 0; 
} 
