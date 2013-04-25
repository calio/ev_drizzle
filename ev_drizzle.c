/*
 * Embed libdrizzle into libev event loop
 */

#include "libdrizzle/drizzle_client.h"
#include "ev.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


#define MAX_HOST_LEN 256


enum client_state {
    CLIENT_QUERY,
    CLIENT_FIELDS,
    CLIENT_ROWS,
    CLIENT_DONE
};

typedef struct client_s {
    enum client_state   state;
    char               *sql;
    int                 len;
    uint64_t            row;
    drizzle_st          drizzle;
    drizzle_con_st     *con;
    drizzle_result_st   result;
    drizzle_column_st   column;
    ev_io               read_w;
    ev_io               write_w;
    struct ev_loop     *loop;
} client_t;

void result_info(drizzle_result_st *result)
{
  printf("Result:     row_count=%" PRId64 "\n"
         "            insert_id=%" PRId64 "\n"
         "        warning_count=%u\n"
         "         column_count=%u\n"
         "        affected_rows=%" PRId64 "\n\n",
         drizzle_result_row_count(result),
         drizzle_result_insert_id(result),
         drizzle_result_warning_count(result),
         drizzle_result_column_count(result),
         drizzle_result_affected_rows(result));
}

void column_info(drizzle_column_st *column)
{
    printf("Field:   catalog=%s\n"
         "              db=%s\n"
         "           table=%s\n"
         "       org_table=%s\n"
         "            name=%s\n"
         "        org_name=%s\n"
         "         charset=%u\n"
         "            size=%u\n"
         "        max_size=%" PRIu64 "\n"
         "            type=%u\n"
         "           flags=%u\n\n",
         drizzle_column_catalog(column), drizzle_column_db(column),
         drizzle_column_table(column), drizzle_column_orig_table(column),
         drizzle_column_name(column), drizzle_column_orig_name(column),
         drizzle_column_charset(column), drizzle_column_size(column),
         (uint64_t)(drizzle_column_max_size(column)), drizzle_column_type(column),
         drizzle_column_flags(column));
}

int get_rows(client_t *client)
{
    drizzle_return_t ret;
    drizzle_field_t  field;
    size_t          offset, length, total;

    printf("get_rows called\n");
    while (1) {
        client->row = drizzle_row_read(&client->result, &ret);
        if (ret == DRIZZLE_RETURN_IO_WAIT) {
            client->row = 0;
            return ret;
        } else if (ret != DRIZZLE_RETURN_OK) {
            printf("get_rows error: %s\n",
                    drizzle_error(&client->drizzle));
            return DRIZZLE_RETURN_OK;
        }

        if (client->row == 0) {
            break;
        }

        printf("read %lld rows\n", client->row);

        while (1) {
            field = drizzle_field_read(&client->result, &offset, &length,
                    &total, &ret);

            if (ret == DRIZZLE_RETURN_IO_WAIT) {
                return ret;
            } else if (ret == DRIZZLE_RETURN_ROW_END) {
                break;
            } else if (ret != DRIZZLE_RETURN_OK) {
                printf("get_field error: %s\n",
                        drizzle_error(&client->drizzle));
            }

            if (field == NULL) {
                printf("    (NULL)");
            } else if (offset > 0) {
                printf("    %.*s", (int) length, field);
            } else {
                printf("    (%d %.*s", (int32_t)total, (int32_t)length, field);
            }

            if (offset + length == total) {
                printf("\n");
            }
        }

        client->row = 0;
        printf("\n");
    }

    drizzle_result_free(&client->result);
    client->state = CLIENT_DONE;
    return DRIZZLE_RETURN_OK;
}

int get_fields(client_t *client)
{
    drizzle_return_t ret;
    drizzle_column_st *column;

    printf("get_fields called\n");

    while (1) {
        column = drizzle_column_read(&client->result, &client->column, &ret);

        if (ret == DRIZZLE_RETURN_IO_WAIT) {
            return ret;
        } else if (ret != DRIZZLE_RETURN_OK) {
            printf("drizzle_column_read error: %s\n",
                    drizzle_error(&client->drizzle));
            exit(1);
        }

        if (column == NULL) {
            break;
        }
        column_info(column);
        drizzle_column_free(column);
    }

    client->state = CLIENT_ROWS;
    return ret;
}

int do_query(client_t *client)
{
    drizzle_return_t ret;
    printf("do_query called\n");

    (void) drizzle_query(client->con, &client->result, client->sql,
            client->len, &ret);

    if (ret == DRIZZLE_RETURN_IO_WAIT) {
        return ret;
    }

    result_info(&client->result);

    if (drizzle_result_column_count(&client->result) == 0) {
        client->state = CLIENT_DONE;
        return DRIZZLE_RETURN_OK;
    }

    ev_io_stop(client->loop, &client->write_w);

    client->state = CLIENT_FIELDS;
    return DRIZZLE_RETURN_OK;
}

int do_process(client_t *client)
{
    drizzle_return_t ret;
    int rc = DRIZZLE_RETURN_OK;

    printf("do_process called\n");

    ret = drizzle_con_wait(&(client->drizzle));
    if (ret != DRIZZLE_RETURN_OK) {
        printf("drizzle_con_wait failed:%s\n", drizzle_error(&client->drizzle));
        return 0;
    }

    while (rc != DRIZZLE_RETURN_IO_WAIT) {
        switch (client->state) {
            case CLIENT_QUERY:
                rc = do_query(client);
                break;
            case CLIENT_FIELDS:
                rc = get_fields(client);
                break;
            case CLIENT_ROWS:
                rc = get_rows(client);
                break;
            case CLIENT_DONE:
                //sleep(10);
                printf("done\n");
                return DRIZZLE_RETURN_OK;
                break;
            default:
                printf("unkonw state: %d\n", (int) client->state);
                break;
        }
    }
    return rc;
}

void write_cb(EV_P_ struct ev_io *w, int revent)
{
    client_t *client = w->data;

    printf("write cb called\n");

    (void) do_process(client);
}

void read_cb(EV_P_ struct ev_io *w, int revent)
{
    client_t *client = w->data;

    printf("read cb called\n");

    (void) do_process(client);
}

int usage(char *name)
{
    printf("Usage: %s [-P port] [-H host] [-h] sql\n", name);
    return 0;
}

int main(int argc, char **argv)
{
    int             ch;
    char           *cmd;
    char            host[MAX_HOST_LEN + 1] = "127.0.0.1";
    unsigned int    port = 3306;
    size_t          len;
    client_t       *client;
    int             fd;
    drizzle_con_st *con, client_con;
    drizzle_return_t ret;

    struct ev_loop *loop = ev_default_loop(ev_recommended_backends() |
            EVBACKEND_KQUEUE);


    cmd = argv[0];

    while ((ch = getopt(argc, argv, "hH:P:")) != -1) {
        switch (ch) {
            case 'H':
                len = strnlen(optarg, MAX_HOST_LEN);
                printf("%zu\n", len);
                memcpy(host, optarg, len);
                host[len] = '\0';
                break;
            case 'P':
                port = (unsigned int) atoi(optarg);
                break;
            case 'h':
            case '?':
            default:
                usage(cmd);
                return 0;
        }
    }

    argc -= optind;
    argv += optind;

    if (argc < 1) {
        usage(cmd);
        return 0;
    }


    client = (client_t *) malloc(sizeof(client_t));
    if (client == NULL) {
        printf("can't alloc memory for client");
        return 1;
    }

    printf("Started. host: %s, port: %d\n", host, port);
    client->sql = argv[0];
    client->len = strlen(client->sql);

    if (drizzle_create(&client->drizzle) == NULL) {
        printf("drizzle_create failed\n");
        return 1;
    }

    drizzle_add_options(&client->drizzle, DRIZZLE_NON_BLOCKING);
    con = drizzle_con_add_tcp(&client->drizzle, &client_con,
            host, port, "root", "", "", DRIZZLE_CON_MYSQL);

    if (con == NULL) {
        printf("drizzle_con_add_tcp failed:%s\n",
                drizzle_error(&client->drizzle));
    }

    ret = drizzle_con_connect(con);
    if (ret != DRIZZLE_RETURN_OK && ret != DRIZZLE_RETURN_IO_WAIT) {
        printf("drizzle_con_connect failed:%s\n",
                drizzle_error(&client->drizzle));
        return;
    }

    fd = drizzle_con_fd(con);

    client->state = CLIENT_QUERY;
    client->con = con;
    client->write_w.data = client;
    client->read_w.data = client;
    client->loop = loop;

    ev_io_init(&client->write_w, write_cb, fd, EV_WRITE);
    ev_io_start(EV_A_ &client->write_w);

    ev_io_init(&client->read_w, read_cb, fd, EV_READ);
    ev_io_start(EV_A_ &client->read_w);

    ev_loop(EV_A_ 0);

    drizzle_free(&client->drizzle);
    return 0;
}
