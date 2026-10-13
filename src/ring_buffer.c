// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"
#include "utils.h"

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	/* TODO: implement ring_buffer_init */
	DIE(!ring || cap == 0, "Ring-ul este null sau nu avem marime pt date");

	// Alocam memorie pentru buffer-ul circular
	ring->data = malloc(cap);
	DIE(!ring->data, "Nu s-a alocat memorie");
	ring->cap = cap;

	// Initializam proprietatile buffer-ului
	ring->read_pos = 0;
	ring->write_pos = 0;
	ring->len = 0;

	// Initializam mutexul si variabilele conditionale
	if (pthread_mutex_init(&ring->mut, NULL) ||
		pthread_cond_init(&ring->ful, NULL) != 0 ||
		pthread_cond_init(&ring->gol, NULL) != 0) {
		free(ring->data);
		pthread_mutex_destroy(&ring->mut);
		return -1;
	}

	// Setam "flag-ul" la true (buffer-ul poate fi folosit)
	ring->oprit = true;

	return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: implement ring_buffer_enqueue */
	DIE(!ring || !data || size == 0, "Ring buffer-ul este null, nu avem date sau size-ul este 0");

	// Blocam accesul concurent folosind mutex-ul
	pthread_mutex_lock(&ring->mut);

	// Asteptam pana cand exista spatiu suficient in buffer
	while (ring->len + size > ring->cap)
		pthread_cond_wait(&ring->ful, &ring->mut);

	// Adaugam datele in buffer la pozitia write_pos
	memcpy(ring->data + ring->write_pos, data, size);
	ring->write_pos = (ring->write_pos + size) % ring->cap; // Calculam noua pozitie de scriere circulara

	// Actualizam lungimea buffer-ului si semnalam ca buffer-ul nu mai este gol. Apoi il deblocam
	ring->len += size;
	pthread_cond_signal(&ring->gol);
	pthread_mutex_unlock(&ring->mut);

	return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: Implement ring_buffer_dequeue */
	DIE(!ring || !data || size == 0, "Ring buffer-ul este null, nu avem date sau size-ul este 0");

	// Blocam accesul concurent folosind mutex-ul
	pthread_mutex_lock(&ring->mut);

	// Asteptam pana cand exista suficiente date pentru citire
	while (ring->len < size) {
		// Daca bufferul este oprit si gol, returnam -1
		if (!ring->oprit && ring->len == 0) {
			pthread_mutex_unlock(&ring->mut);
			return -1;
		}
		pthread_cond_wait(&ring->gol, &ring->mut);
	}

	// Copiem datele din buffer la pozitia read_pos
	memcpy(data, ring->data + ring->read_pos, size);
	ring->read_pos = (ring->read_pos + size) % ring->cap; // Calculam noua pozitie de citire circulara

	// Actualizam ca mai sus, dar acum semnalam ca nu mai este ful
	ring->len -= size;
	pthread_cond_signal(&ring->ful);
	pthread_mutex_unlock(&ring->mut);

	return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_destroy */
	DIE(!ring, "Ring buffer-ul este null");

	// Distrugem variabilele conditionale si mutex-ul, apoi eliberam memoria alocata
	pthread_cond_destroy(&ring->ful);
	pthread_cond_destroy(&ring->gol);
	pthread_mutex_destroy(&ring->mut);
	free(ring->data);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_stop */
	DIE(!ring, "Ring buffer-ul este null");

	// Blocam accesul mutex-ului, stam flagul la false si notificam toate thread-urile blocate
	// Apoi il deblocam
	pthread_mutex_lock(&ring->mut);
	ring->oprit = false;
	pthread_cond_broadcast(&ring->gol);
	pthread_mutex_unlock(&ring->mut);
}
