// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

typedef struct sortare {
	// Actiunea din pachet
	int action;
	// Hash-uil calculat din pachet
	unsigned long hash;
	// Timpul din pachet
	unsigned long timestamp;
} sortare;

// Functia pentru comparare in qsort
int sortar(const void *a, const void *b)
{
	sortare test1 = *((sortare *)a);
	sortare test2 = *((sortare *)b);

	if (test1.timestamp > test2.timestamp)
		return 1;
	else if (test1.timestamp < test2.timestamp)
		return -1;
	return 0;
}

// Vectorul de structuri pentru sortare
sortare so[10000];

// Marimea vectorului actuala
size_t size_so;

void consumer_thread(so_consumer_ctx_t *ctx)
{
	/* TODO: implement consumer thread */
	//Buffer-ul pentru a stoca datele citite din ring buffer
	char buffer[256];

	// Acest size este pentru a verfica in while conditia
	ssize_t size;

	// Citim incontinuu din ring buffer  pana cand nu mai exista date
	// Cele de mai jos, multe sunt luate din serial.c
	while ((size = ring_buffer_dequeue(ctx->producer_rb, buffer, 256)) > 0) {
		struct so_packet_t *pkt = (struct so_packet_t *)buffer;


		//Procesam pachetul si obtinem rezultatul actiunii
		int action = process_packet(pkt);

		// Calculam hash-ul pachetului
		unsigned long hash = packet_hash(pkt);

		// Extragem timestamp-ul din header-ul pachet
		unsigned long timestamp = pkt->hdr.timestamp;

		// Blocam mutex-ul
		pthread_mutex_lock(ctx->mutex);

		// Lipim in so toate datele din pachete
		so[size_so].action = action;
		so[size_so].hash = hash;
		so[size_so].timestamp = timestamp;

		// Crestem dimensiunea
		size_so++;

		// Daca s-a ajuns la 1100 de pachete, le procesam
		if (size_so == 100) {
			// Le sortam
			qsort(so, size_so, sizeof(sortare), sortar);

			// Scriem primele 50 de elemente
			for (size_t i = 0; i < size_so / 2; i++) {
				char out[256];

				// Acestea sunt luate din serial
				int len = snprintf(out, 256, "%s %016lx %lu\n",
					RES_TO_STR(so[i].action), so[i].hash, so[i].timestamp);

				write(ctx->out, out, len);
			}

			// Mutam pachetele neafisate spre stanga
			memmove(so, so + 50, (size_so - 50) * sizeof(sortare));
			size_so -= 50;
		}
		// Deblocam mutexul
		pthread_mutex_unlock(ctx->mutex);
	}

	// La final afisam pachetele ramase la fel ca mai sus
	if (size_so <= 50) {
		pthread_mutex_lock(ctx->mutex);

		qsort(so, size_so, sizeof(sortare), sortar);
		for (size_t i = 0; i < size_so; i++) {
			char out[256];

			int len = snprintf(out, 256, "%s %016lx %lu\n",
				RES_TO_STR(so[i].action), so[i].hash, so[i].timestamp);


			write(ctx->out, out, len);
		}

		size_so = 0;
		pthread_mutex_unlock(ctx->mutex);
	}
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{
	// Alocam spatiul pentru mutex
	pthread_mutex_t *mutex = malloc(sizeof(pthread_mutex_t));

	DIE(mutex == NULL, "Eroare la alocarea spatiului pentru mutex");

	// Initalizam mutex-ul
	pthread_mutex_init(mutex, NULL);

	// Deschidam fisierul de afisare
	int out = open(out_filename, O_RDWR|O_CREAT|O_TRUNC, 0666);

	for (int i = 0; i < num_consumers; i++) {
		/*
		 * TODO: Launch consumer threads
		 **/

		// Alocam spatiu pentru consumer
		so_consumer_ctx_t *ctx = malloc(sizeof(so_consumer_ctx_t));

		DIE(ctx == NULL, "create_consumer nu a putut aloca memorie");

		// Initializam contextul thread-ului
		ctx->producer_rb = rb;
		ctx->out = out;
		ctx->mutex = mutex;

		//Cream thread-ul si ii atribuim functia
		pthread_create(&tids[i], NULL, (void *)consumer_thread, ctx);
	}

	return num_consumers;
}
