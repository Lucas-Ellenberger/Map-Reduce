#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "hash.h"
#include "kvlist.h"

// used to send more parameters while using threads
typedef struct mapper_package mapper_package;

struct mapper_package {
  mapper_t* func;
  kvlist_t* input;
  kvlist_t* output;
};

mapper_package* mapper_package_new(mapper_t* func, kvlist_t* input,
                                   kvlist_t* output) {
  mapper_package* new_package = malloc(sizeof(mapper_package));
  new_package->func = func;
  new_package->input = input;
  new_package->output = output;
  return new_package;
}

// used to send more parameters while using threads
typedef struct reducer_package reducer_package;

struct reducer_package {
  reducer_t* func;
  kvlist_t* input;
  kvlist_t* output;
};

reducer_package* reducer_package_new(reducer_t* func, kvlist_t* input,
                                     kvlist_t* output) {
  reducer_package* new_package = malloc(sizeof(reducer_package));
  new_package->func = func;
  new_package->input = input;
  new_package->output = output;
  return new_package;
}

void* list_mapper(void* argv) {
  mapper_package* package = argv;
  kvlist_iterator_t* itor = kvlist_iterator_new(package->input);
  kvpair_t* curr;
  while ((curr = kvlist_iterator_next(itor)) != NULL) {
    (*(package->func))(curr, package->output);
  }
  kvlist_iterator_free(&itor);
  return NULL;
}

void* list_reducer(void* argv) {
  reducer_package* package = argv;
  kvlist_sort(package->input);
  kvlist_iterator_t* itor = kvlist_iterator_new(package->input);
  kvpair_t* curr = kvlist_iterator_next(itor);
  if (curr == NULL) {
    kvlist_iterator_free(&itor);
    return NULL;
  }
  kvlist_t* word_list = kvlist_new();
  char* curr_key = curr->key;
  kvlist_append(word_list, kvpair_clone(curr));
  while ((curr = kvlist_iterator_next(itor)) != NULL) {
    if (strcmp(curr->key, curr_key) == 0) {
      kvlist_append(word_list, kvpair_clone(curr));
    } else {
      (*(package->func))(curr_key, word_list, package->output);
      kvlist_free(&word_list);
      word_list = kvlist_new();
      kvlist_append(word_list, kvpair_clone(curr));
      curr_key = curr->key;
    }
  }
  (*(package->func))(curr_key, word_list, package->output);
  kvlist_free(&word_list);
  kvlist_iterator_free(&itor);
  return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
  // Input: kvlist_t* input

  // SPLIT PHASE:
  // Split the input list into num_mapper smaller lists.
  kvlist_t** mapper_in_list = calloc(num_mapper, sizeof(kvlist_t*));
  kvlist_t** mapper_out_list = calloc(num_mapper, sizeof(kvlist_t*));
  for (size_t i = 0; i < num_mapper; i++) {
    mapper_in_list[i] = kvlist_new();
    mapper_out_list[i] = kvlist_new();
  }
  kvlist_iterator_t* itor = kvlist_iterator_new(input);
  kvpair_t* curr;
  size_t i = 0;
  while ((curr = kvlist_iterator_next(itor)) != NULL) {
    kvlist_append(mapper_in_list[i], kvpair_clone(curr));
    i++;
    if (i == num_mapper) {
      i = 0;
    }
  }

  // Free iterator
  kvlist_iterator_free(&itor);

  // MAP PHASE:
  // Spawn num_mapper threads and execute the provided map function.
  // Call mapper on num_mapper threads

  pthread_t* mapper_threads = calloc(num_mapper, sizeof(pthread_t*));
  struct mapper_package** mapper_package_array =
      calloc(num_mapper, sizeof(mapper_package*));
  for (size_t i = 0; i < num_mapper; i++) {
    mapper_package_array[i] =
        mapper_package_new(&mapper, mapper_in_list[i], mapper_out_list[i]);
    pthread_create(&(mapper_threads[i]), NULL, &list_mapper,
                   (struct mapper_package*)mapper_package_array[i]);
  }

  // SHUFFLE PHASE:
  // Shuffle mapper results to num_reducer lists.
  // Create a hash with num reducer buckets (kvlists)
  kvlist_t** reducer_in_list = calloc(num_reducer, sizeof(kvlist_t*));
  kvlist_t** reducer_out_list = calloc(num_reducer, sizeof(kvlist_t*));
  for (size_t i = 0; i < num_reducer; i++) {
    reducer_in_list[i] = kvlist_new();
    reducer_out_list[i] = kvlist_new();
  }

  // JOIN THREADS!!!
  for (size_t i = 0; i < num_mapper; i++) {
    pthread_join(mapper_threads[i], NULL);
  }

  free(mapper_threads);

  for (size_t i = 0; i < num_mapper; i++) {
    kvlist_free(&mapper_in_list[i]);
    free(mapper_package_array[i]);
  }
  free(mapper_in_list);
  free(mapper_package_array);

  for (size_t i = 0; i < num_mapper; i++) {
    kvlist_iterator_t* itor = kvlist_iterator_new(mapper_out_list[i]);
    kvpair_t* curr;
    while ((curr = kvlist_iterator_next(itor)) != NULL) {
      unsigned long hashvalue = hash(curr->key);
      kvlist_append(reducer_in_list[hashvalue % num_reducer],
                    kvpair_clone(curr));
    }
    kvlist_iterator_free(&itor);
  }

  for (size_t i = 0; i < num_mapper; i++) {
    kvlist_free(&(mapper_out_list[i]));
  }
  free(mapper_out_list);

  // REDUCE PHASE:
  // Spawn num_reducer threads and execute the provided reduce function.
  // repeat in, out, func package for reduce
  struct reducer_package** reducer_package_array =
      calloc(num_reducer, sizeof(reducer_package*));
  pthread_t* reduce_threads = calloc(num_reducer, sizeof(pthread_t*));
  for (size_t i = 0; i < num_reducer; i++) {
    reducer_package_array[i] =
        reducer_package_new(&reducer, reducer_in_list[i], reducer_out_list[i]);
    pthread_create(&(reduce_threads[i]), NULL, &list_reducer,
                   (struct reducer_package*)reducer_package_array[i]);
  }

  // JOIN THREADS!!!
  for (size_t i = 0; i < num_reducer; i++) {
    pthread_join(reduce_threads[i], NULL);
  }

  free(reduce_threads);

  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_free(&reducer_in_list[i]);
  }
  free(reducer_in_list);

  // Output
  // You need to store the results in the output list passed as an argument.
  // Use kvlist_extend to move pairs to output.
  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_extend(output, reducer_out_list[i]);
  }

  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_free(&reducer_out_list[i]);
  }
  free(reducer_out_list);

  for (size_t i = 0; i < num_reducer; i++) {
    free(reducer_package_array[i]);
  }
  free(reducer_package_array);

  return;
}
