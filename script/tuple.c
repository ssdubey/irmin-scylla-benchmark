#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "cassandra.h"

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
  return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError prepare_query(CassSession* session, const char* query, const CassPrepared** prepared) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;

  future = cass_session_prepare(session, query);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    *prepared = cass_future_get_prepared(future);
  }

  cass_future_free(future);

  return rc;
}



CassError insert_into_tuple(CassSession* session) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  printf("inside insert into tuple");
  const char* query = "INSERT INTO irmin_scylla.append_only (key, value) VALUES (?, ?)";
  
  statement = cass_statement_new(query, 2);

////////////////////////////////////////////

	int entrycount = 300;
	int keysize = 10;
	int valuesize = 50;
	srand(time(0));

	char keys[entrycount][1000];
	char values[entrycount][1000];

	char *string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	char tempchar[keysize+1];

	int n = 0;
	int i =0;


	for (n=0;n<entrycount;n++){
		for (i =0; i<10;i++){
			int key = rand() % 62;
			tempchar[i]=string[key];
		}
		tempchar[i]='\0';
		strcpy(keys[n], tempchar);
	}

	char tempcharval[valuesize+1];
	for (n=0;n<entrycount;n++){
		for (i =0; i<50;i++){
			int key = rand() % 62;
			tempcharval[i]=string[key];
		}
	tempcharval[i]='\0';
	strcpy(values[n], tempcharval);
	
	}
////////////////////////////////////////    
    
    
	 for (int i = 0; i<entrycount; i++){ 
	  cass_statement_bind_string(statement, 0, values[i]);
	  cass_statement_bind_string(statement, 1, keys[i]);
	  
	  future = cass_session_execute(session, statement);
	  cass_future_wait(future);

	  rc = cass_future_error_code(future);
	  if (rc != CASS_OK) {
		print_error(future);
	  }
	}
	  cass_future_free(future);
	  cass_statement_free(statement);
	  
  return rc;
}

int main(int argc, char* argv[]) {
  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }
  cluster = create_cluster(hosts);


  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  insert_into_tuple(session);
  
  cass_cluster_free(cluster);
  cass_session_free(session);


  return 0;
}
