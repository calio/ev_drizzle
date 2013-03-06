#define _DEBUG_ 1

#include "libdrizzle/drizzle_server.h"
#include "ev.h"
#include "debug.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>



typedef enum connection_state {
    INIT         = 0,
    HANDSHAKE_WRITE,
    HANDSHAKE_READ,
    HANDSHAKE_DONE,
    PREPARE_COMMAND,
} con_state;

typedef struct client_st {
    drizzle_st      *drizzle;
    drizzle_con_st  *dc;
    struct ev_loop  *loop;
} client_t;

typedef struct connection_st {
    int              fd;
    ev_io            read_w;
    ev_io            write_w;
    client_t        *client;
    drizzle_con_st  *dc;
    drizzle_result_st   *result;
    con_state        state;
} connection_t;


void usage(const char *cmd)
{
    printf("Usage: %s [-h <host>] [-p <port>]\n", cmd);
}

void stop_watch_write(connection_t *con)
{
    dbgin();
    ev_io_stop(con->client->loop, &con->write_w);
}

void stop_watch_read(connection_t *con)
{
    dbgin();
    ev_io_stop(con->client->loop, &con->read_w);
}

void do_process(connection_t *con)
{
    drizzle_st          *drizzle;
    drizzle_return_t     ret;
    drizzle_con_st      *dc = con->dc;
    drizzle_command_t    command;
    uint8_t             *data = NULL;
    size_t               total;

    dbgin();

    drizzle = con->client->drizzle;

    /*
    ret = drizzle_con_wait(drizzle);
    if (ret != DRIZZLE_RETURN_OK) {
        printf("drizzle_con_wait failed:%s\n", drizzle_error(drizzle));
        return;
    }
    */

    while (1) {
        switch (con->state) {
            case INIT:
                /* Handshake packets */
                drizzle_con_set_protocol_version(dc, 10);
                drizzle_con_set_server_version(dc, "ev_drizzle_server demo 0.0.1");
                drizzle_con_set_thread_id(dc, 1);
                drizzle_con_set_scramble(dc, (const uint8_t *)"ABCDEFGHIJKLMNOPQRST");
                drizzle_con_set_capabilities(dc, DRIZZLE_CAPABILITIES_NONE);
                drizzle_con_set_charset(dc, 8);
                drizzle_con_set_status(dc, DRIZZLE_CON_STATUS_NONE);
                drizzle_con_set_max_packet_size(dc, DRIZZLE_MAX_PACKET_SIZE);

                con->state = HANDSHAKE_WRITE;
                break;

            case HANDSHAKE_WRITE:
                printf("handshake write\n");

                ret = drizzle_handshake_server_write(dc);

                if (ret == DRIZZLE_RETURN_IO_WAIT) {
                    return;
                }

                if (ret == DRIZZLE_RETURN_OK) {
                    stop_watch_write(con);
                    con->state = HANDSHAKE_READ;
                    break;
                }
                printf("drizzle_handshake_server_write error:%s",
                        drizzle_error(drizzle));
                exit(1);
                break;
            case HANDSHAKE_READ:
                printf("handshake read\n");

                /* prepare libdrizzle internal event */
                dc->revents |= POLLIN;

                ret = drizzle_handshake_client_read(dc);

                if (ret == DRIZZLE_RETURN_IO_WAIT) {
                    return;
                }

                if (ret == DRIZZLE_RETURN_OK) {
                    con->state = HANDSHAKE_DONE;
                    break;
                }
                printf("drizzle_handshake_server_read error:%s",
                        drizzle_error(drizzle));
                exit(1);
                break;

            case HANDSHAKE_DONE:
                printf("handshake done\n");
                con->result = (drizzle_result_st *)
                    malloc(sizeof(drizzle_result_st));
                if (drizzle_result_create(dc, con->result) == NULL) {
                    printf("drizzle_result_create error:%s",
                            drizzle_error(drizzle));
                    exit(1);
                }

                ret = drizzle_result_write(dc, con->result, true);
                if (ret != DRIZZLE_RETURN_OK) {
                    printf("drizzle_result_write error:%s",
                            drizzle_error(drizzle));
                    exit(1);
                }
                con->state = PREPARE_COMMAND;
                break;
            case PREPARE_COMMAND:
                printf("prepare command\n");

                /* prepare libdrizzle internal event */
                dc->revents |= POLLIN;

                drizzle_result_free(dc->result);

                data = (uint8_t *) drizzle_con_command_buffer(dc, &command,
                        &total, &ret);


                if (ret == DRIZZLE_RETURN_LOST_CONNECTION ||
                        (ret == DRIZZLE_RETURN_OK &&
                         command == DRIZZLE_COMMAND_QUIT))
                {
                    free(data);
                    return;
                }

                if (ret == DRIZZLE_RETURN_IO_WAIT) {
                    return;
                }

                printf("Command data:%s\n", data);

                if (drizzle_result_create(dc, con->result) == NULL) {
                    printf("drizzle_result_create error:%s",
                            drizzle_error(drizzle));
                    exit(1);
                }

                if (command != DRIZZLE_COMMAND_QUERY) {
                    printf("command is not QUERY command\n");
                    ret = drizzle_result_write(dc, dc->result, true);
                    if (ret != DRIZZLE_RETURN_OK) {
                        printf("drizzle_result_write error:%s",
                                drizzle_error(drizzle));
                        exit(1);
                    }
                    continue;
                }

                ret = drizzle_result_write(dc, dc->result, true);
                if (ret != DRIZZLE_RETURN_OK) {
                    printf("drizzle_result_write error:%s",
                            drizzle_error(drizzle));
                    exit(1);
                }

                return;
                break;
            default:
                printf("Unsupported state %d\n", con->state);
                //sleep(10);
        }
    }

}

void write_cb(EV_P_ struct ev_io *w, int revent)
{
    connection_t *con = (connection_t *) w->data;

    dbgin();

    do_process(con);
}

void read_cb(EV_P_ struct ev_io *w, int revent)
{
    connection_t *con = (connection_t *) w->data;

    dbgin();

    do_process(con);
}


int add_connection(client_t *client, connection_t *con)
{
    dbgin();

    ev_io_init(&con->read_w, read_cb, con->fd, EV_READ);
    ev_io_init(&con->write_w, write_cb, con->fd, EV_WRITE);
    con->read_w.data = con;
    con->write_w.data = con;
    ev_io_start(client->loop, &con->read_w);
    ev_io_start(client->loop, &con->write_w);

    return 0;
}

void accept_cb(EV_P_ struct ev_io *w, int revent)
{
    client_t *client = (client_t *) w->data;
    drizzle_con_st     *dc = (drizzle_con_st *) malloc(sizeof(drizzle_con_st));
    drizzle_return_t    ret;
    int                 fd;
    connection_t       *con;

    dbgin();

    /* Manually set connection to ready */
    drizzle_con_add_options(client->dc, DRIZZLE_CON_IO_READY);

    drizzle_con_accept(client->drizzle, dc, &ret);
    if (ret != DRIZZLE_RETURN_OK)
    {
        if (ret == DRIZZLE_RETURN_IO_WAIT) {
            printf("io_wait\n");
            return;
        }
        printf("drizzle_con_accpet error:%s\n", drizzle_error(client->drizzle));
        return;
    }

    fd = drizzle_con_fd(dc);
    printf("Accepted. fd:%d\n", fd);

    con = (connection_t *) calloc(1, sizeof(connection_t));
    con->fd = fd;
    con->client = client;
    con->dc = dc;

    add_connection(client, con);

}

void do_finalize(connection_t *con)
{
    drizzle_con_st *dc = con->dc;

    dbgin();

    drizzle_con_free(dc);

    free(dc);
    free(con);
    printf("Close fd.\n");
}


int main(int argc, char *argv[])
{
    int             c;
    drizzle_st      drizzle;
    drizzle_con_st  con_listen;
    in_port_t       port = 0;
    char           *host = "localhost";
    struct ev_loop *loop = ev_default_loop(ev_recommended_backends() |
            EVBACKEND_KQUEUE);
    ev_io           accept_w;
    int             accept_fd;
    client_t        client;

    dbgin();

    while ((c = getopt(argc, argv, "p:h:")) != -1) {
        switch (c) {
            case 'p':
                port = (in_port_t) atoi(optarg);
                break;
            case 'h':
                host = optarg;
                break;
            default:
                usage(argv[0]);
                return 1;
        }
    }

    if (drizzle_create(&drizzle) == NULL) {
        printf("drizzle_create error: NULL\n");
        return 1;
    }

    drizzle_add_options(&drizzle, DRIZZLE_FREE_OBJECTS);
    drizzle_add_options(&drizzle, DRIZZLE_NON_BLOCKING);
    drizzle_set_verbose(&drizzle, DRIZZLE_VERBOSE_NEVER);

    if (drizzle_con_create(&drizzle, &con_listen) == NULL) {
        printf("drizzle_con_create error: NULL\n");
        return 1;
    }

    drizzle_con_add_options(&con_listen, DRIZZLE_CON_LISTEN);
    drizzle_con_set_tcp(&con_listen, host, port);

    /* Add mysql protocol support */
    drizzle_con_add_options(&con_listen, DRIZZLE_CON_MYSQL);

    if (drizzle_con_listen(&con_listen) != DRIZZLE_RETURN_OK)
    {
        printf("drizzle_con_listen:%s\n", drizzle_error(&drizzle));
        return 1;
    }


    client.drizzle = &drizzle;
    client.dc = &con_listen;


    accept_fd = drizzle_con_fd(&con_listen);

    ev_io_init(&accept_w, accept_cb, accept_fd, EV_READ);
    ev_io_start(EV_A_ &accept_w);

    client.loop = loop;
    accept_w.data = &client;

    ev_loop(EV_A_ 0);

    drizzle_con_free(&con_listen);
    drizzle_free(&drizzle);

    return 0;
}
