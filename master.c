#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h> 
#include <sys/types.h> 
#include <sys/wait.h>
#include <signal.h>
#include <netinet/in.h> 
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h> 
#include <fcntl.h> // non-blocking sockets
#include <stdbool.h>
#include <dirent.h>

#define TRUE 1
#define FALSE 0
#define BUFSIZE 1024
#define PORT 8080
#define SA struct sockaddr 

// threads
pthread_t listenthread;
pthread_t mapthread;
pthread_t reducethread;

char buffer[BUFSIZE];
char buf[BUFSIZE];
char chunk[BUFSIZE];
char filename[20];
char word[20];
int l;  // lenght of filename
int filesize;
int chunksize;
FILE *f;
DIR *d;
struct dirent *dir;

int s = 0;
int sockfd;
int connfd;
int len;
int sockets[10];
int input_fd;
ssize_t ret_in;
int n;
struct sockaddr_in servaddr, cli,address; 
int slave_socket[30];
int max_slaves;
int total_slaves = 0;
int activity, i;
int valread;
int sd, max_sd;
fd_set fds;
int optval;
int ret;

int totalcount;
int c;

typedef struct MSG
{
    char mbuf[BUFSIZE];
    char mword[20];
    int mcount;
}MSG;

struct MSG msg;

void error(char *msg) {
	perror(msg);
	exit(1);
}


void* listento(void* argv) {

	for(i = 0; i < max_slaves; i++) {
		slave_socket[i] = 0;
	}

	// socket create and verification 
	sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0); 
	if (sockfd == -1) { 
		printf("Socket creation failed...\n"); 
		exit(0); 
	} 
	else {
		printf("Socket successfully created..\n"); 
	}
		
	optval = 1;
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (const void *)&optval, sizeof(int));
	bzero(&servaddr, sizeof(servaddr)); 

	// assign IP, PORT 
	servaddr.sin_family = AF_INET; 
	servaddr.sin_addr.s_addr = htonl(INADDR_ANY); 
	servaddr.sin_port = htons(PORT); 

	// Binding newly created socket to given IP and verification 
	if ((bind(sockfd, (SA*)&servaddr, sizeof(servaddr))) != 0) { 
		printf("Socket bind failed...\n"); 
		exit(0); 
	} 
	else {
		printf("Socket successfully binded..\n"); 
	}
	
	// Now master is ready to listen and verification 
	if ((listen(sockfd, 5)) != 0) { 
		printf("Listen failed...\n"); 
		exit(0); 
	} 
	else {
		printf("Master listening....\n"); 
	}
	
	len = sizeof(cli); 
	FD_ZERO(&fds);
	FD_SET(sockfd, &fds);
	max_sd = sockfd;
	
	while(TRUE) {	
		//add child sockets to set
		for (i = 0; i < max_slaves; i++) {
    		sd = slave_socket[i];  // socket descriptor
    		
    		// if valid socket descriptor, add to read list
    		if(sd > 0) { 
    			FD_SET(sd,&fds);
    		}
    		// highest file descriptor number, need it for the select function
    		if(sd > max_sd)  {
    			max_sd = sd;
    		}
     	}
     	
     	// wait for any activity on one of the sockets
     	activity = select(max_sd + 1, &fds, NULL, NULL, NULL);
     	if((activity < 0) && (errno != EINTR)) {
     		printf("Select error\n");
     	}
     	
     	// if something happened on the master socket, it's an incoming connection
     	if(FD_ISSET(sockfd,&fds)) {
   	    	connfd = accept4(sockfd,(SA*) &cli, &len, SOCK_NONBLOCK);
    		if(connfd < 0) {
				if(errno == EAGAIN || errno == EWOULDBLOCK)
					continue; 
			} 
		}
		
		c = TRUE;
		printf("Master accept the slave...\n"); 
		printf("Connection accepted from %s:%d  socket fd is %d\n",inet_ntoa(cli.sin_addr), ntohs(cli.sin_port), connfd);
		total_slaves += 1;
		
        // add new socket to array of socket
		for(i = 0 ; i < max_slaves; i++) {
         	if(slave_socket[i] == 0) {
           		slave_socket[i] = connfd;
           		printf("Adding to list of sockets as %d\n",i);
           		break;
           	}
        }
        
        sockets[s] = connfd;
        s += 1;
	} // end while
}


void* map_thread(void* argv) {
	d = opendir(".");
	if(d) {
		while( (dir = readdir(d)) != NULL) {
		    strcpy(filename,dir->d_name);
		    l = strlen(filename);
		    if(filename[l-1] == 't' && filename[(l-2)] == 'x' && filename[(l-3)] =='t' && filename[(l-4)] == '.') {
				printf("%s\n", filename);
				break;
			}
		}
		closedir(d);
	}
	
	printf("File Name: %s\n",filename);
	
	f = fopen(filename,"rb");
	fseek(f,0,SEEK_END);
	filesize = ftell(f);
	fseek(f,0,SEEK_SET);
	printf("File size: %d\n",filesize);
	fclose(f);
	
	chunksize = filesize / s;
	printf("Chunk size is %d\n\n",chunksize);
    printf("Number of sockets is %d\n",s);
    for (int i = 0; i < s; i++) {
    	printf("Socket %d is %d\n",i+1,sockets[i]); 
    }    

    // send chunks to slaves
    input_fd = open(filename,O_RDONLY);
    if(input_fd == -1) {
    	perror("open");
    }
    
    lseek(ret_in,0,SEEK_SET);
    for (int i = 0; i < s; i++) {
    	bzero(chunk,chunksize);
    	if ((ret_in = read(input_fd,&chunk,chunksize)) > 0) {
    		bzero((char *) &msg,sizeof(MSG));
    		strcpy(msg.mbuf,chunk);
    		strcpy(msg.mword," ");
    		msg.mcount = 0;
    		n = write(sockets[i],&msg,sizeof(MSG));
    		if(n < 0) {
    			error("Error writing to socket");
    		}
    		
    		bzero(chunk,chunksize);
    		lseek(ret_in,chunksize,SEEK_SET);
    	}
    }
    
    sleep(3);
    // map tasks
    bzero((char *) &msg,sizeof(MSG));
    strcpy(msg.mbuf," ");
    strcpy(msg.mword,"cat");
    msg.mcount = 0;
    
    for (int i = 0; i < s; i++) {
   		n = write(sockets[i],&msg,sizeof(MSG));
   		if(n<0) {
   			error("Error writing to socket");
   		}
   	}
}


void* reduce_thread(void* argv) {
	totalcount = 0;
	int j = 0;
	
	while (j < s) { 
		bzero((char *)&msg, sizeof(MSG)); 

		// read the message from slave and get the count 
		n = read(sockets[j], &msg, sizeof(MSG));
		if (n < 0) {
    		if(errno == EAGAIN || errno == EWOULDBLOCK)
    		{
    			continue;
       		}
       	} else {
			totalcount += msg.mcount;
    		printf("Count recieved from slave-socket %d for word (%s): %d \n",sockets[j],word,msg.mcount); 
    		j += 1;
    	}
	}
	
	printf("Work to find count of word (%s) done!\n",word);
	printf("\nTotal count: %d times!\n",totalcount); 

}


int main(int argc, char* argv) { 
	
	strcpy(word,"cat");
	c = FALSE;
	
    ret = pthread_create(&listenthread, NULL, listento, NULL);
    printf("Listenthread thread returns %d\n\n",ret);
    
    while(c == FALSE) {
    	;
    }
    
    sleep(10);
	printf("Total slaves = %d\n",total_slaves);
	
	ret = pthread_create(&mapthread, NULL, map_thread, NULL);
	pthread_join(mapthread,NULL);
    printf("mapthread thread returns %d\n\n",ret);
    sleep(3);
  
	ret = pthread_create(&reducethread, NULL, reduce_thread, NULL);
	pthread_join(reducethread,NULL);
    printf("reducethread thread returns %d\n\n",ret);
	
	//close the socket 
	printf("\nClosing the connection...\n");
	close(sockfd); 
	
	return 0;
} 

