#define ELPP_THREAD_SAFE

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "indexes.hpp"
#include <iostream>

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

#include "rlz_utils.hpp"
#include "factor_storage.hpp"

#include "mongoose.h"

static struct mg_serve_http_opts s_http_server_opts;
static factorization_statistics* s_fs = nullptr;
static sdsl::int_vector<8>* s_dict = nullptr;

struct cell_stat {
    uint64_t start;
    uint64_t stop;
    uint64_t freq;
    uint64_t quantile;
    uint64_t bytes_per_cell;
};

std::string 
compute_json_stats(uint64_t start,uint64_t stop,uint64_t num_cells)
{
    if(start == 0 && stop == 0) {
        stop = s_fs->dict_usage.size()-1;
    }
    auto num_bytes = stop-start;
    auto bytes_per_cell = num_bytes / num_cells;
    if(bytes_per_cell == 0) bytes_per_cell = 1;

    std::vector<cell_stat> cell_stats;

    for(size_t i=start;i<=stop;i+=bytes_per_cell) {
        cell_stat cs;
        cs.start = i;
        cs.stop = i+bytes_per_cell-1;
        cs.bytes_per_cell = bytes_per_cell;
        cs.freq = 0;
        for(size_t j=0;j<bytes_per_cell;j++) {
            if(i+j == s_fs->dict_usage.size()) {
                cs.bytes_per_cell = j;
                cs.stop = i+j;
                break;
            }
            cs.freq += s_fs->dict_usage[i+j];
        }
        if(cs.freq > 1000000000) { // 100M
            cs.quantile = 9;
        }
        if(cs.freq <= 1000000000) { // 100M
            cs.quantile = 8;
        }
        if(cs.freq <= 10000000) { // 10M
            cs.quantile = 7;
        }
        if(cs.freq <= 1000000) {
            cs.quantile = 6;
        }
        if(cs.freq <= 100000) {
            cs.quantile = 5;
        }
        if(cs.freq<= 10000) {
            cs.quantile = 4;
        }
        if(cs.freq <= 1000) {
            cs.quantile = 3;
        }
        if(cs.freq <= 100) {
            cs.quantile = 2;
        }
        if(cs.freq <= 10) {
            cs.quantile = 1;
        }
        if(cs.freq == 0) {
            cs.quantile = 0;
        }
        cell_stats.push_back(cs);
    }

    /* create json object */
    std::string json_response;
    json_response += "{\n";
    json_response += "\t\"data\":[\n";
    for(size_t i=0;i<cell_stats.size();i++) {
        const auto& cs = cell_stats[i];
        json_response += "\t\t\t{";
        json_response += "\"id\":" + std::to_string(i) + ",";
        json_response += "\"start\":" + std::to_string(cs.start) + ",";
        json_response += "\"stop\":" + std::to_string(cs.stop) + ",";
        json_response += "\"freq\":" + std::to_string(cs.freq) + ",";
        json_response += "\"bytes_per_cell\":" + std::to_string(cs.bytes_per_cell) + ",";
        if(cs.bytes_per_cell <= 500) {
            std::string content;
            for(size_t j=cs.start;j<=cs.stop;j++) {
                auto sym = (*s_dict)[j];
                if(!isprint(sym)) content += "?";
                else if(isspace(sym)) content += " ";
                else if(sym == '\\') content += "\\\\";
                else if(sym == '"') content += "\\\"";
                else content += sym;
            }
            json_response += "\"content\": \"" + content + "\",";
        }
        json_response += "\"quantile\":" + std::to_string(cs.quantile);
        if(i==cell_stats.size()-1) json_response += "}\n";
        else json_response +=  "},\n";
    }
    json_response +=  "\t]\n";
    json_response +=  "}\n";
    return json_response;
}



static void rpc_dict_stats(struct mg_connection *nc,http_message *hm) {
    static std::map<std::string,std::string> cache;
    char start_pos[1000] = {0};
    char end_pos[1000] = {0};
    char num_cells_str[1000] = {0};
    /* Get query variables */
    mg_get_http_var(&hm->query_string, "start", start_pos, sizeof(start_pos));
    mg_get_http_var(&hm->query_string, "end", end_pos, sizeof(end_pos));
    mg_get_http_var(&hm->query_string, "numcells", num_cells_str, sizeof(num_cells_str));

    uint64_t start = strtoul(start_pos,NULL,10);
    uint64_t end = strtoul(end_pos,NULL,10);
    uint64_t num_cells = strtoul(num_cells_str,NULL,10);

    LOG(INFO) << "start = " << start << " end = " << end << " num_cells = " << num_cells;

    std::string cache_key = std::to_string(start)+"-"+std::to_string(end)+"-"+std::to_string(num_cells);
    auto itr = cache.find(cache_key);
    std::string json_response;
    if(itr != cache.end()) {
        json_response = itr->second;
    } else {
        json_response = compute_json_stats(start,end,num_cells);
        cache[cache_key] = json_response;
    }

    // printf("HTTP/1.1 200 OK\r\n"
    //         "Cache-Control: no-cache\r\n"
    //         "Content-Type: application/json\r\n"
    //         "Content-Length: %lu\r\n"
    //         "\r\n"
    //         "%s",
    //         json_response.size(),
    //         json_response.c_str());

    /* Send headers and json body */
    mg_printf(nc,
            "HTTP/1.1 200 OK\r\n"
            "Cache-Control: no-cache\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: %d\r\n"
            "\r\n"
            "%s",
            json_response.size(),
            json_response.c_str());
    LOG(INFO) << "SEND DONE";
}

static void ev_handler(struct mg_connection *nc, int ev, void *ev_data) {
  struct http_message *hm = (struct http_message *) ev_data;
  if(ev == NS_HTTP_REQUEST) {
    std::string req(hm->message.p,hm->message.len);
    LOG(INFO) << req;
      if (mg_vcmp(&hm->uri, "/rlz_dict_stats") == 0) {
        rpc_dict_stats(nc, hm);                    /* Handle RESTful call */
      } else {
        mg_serve_http(nc, hm, s_http_server_opts);  /* Serve static content */
      }
  }
}

#include "experiments/rlz_types_www16.hpp"

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    auto args = utils::parse_args(argc, argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    collection col(args.collection_dir);

    /* create rlz index */
    auto rlz_store = rlz_type_zzz_greedy_sp::builder{}
                         .set_rebuild(args.rebuild)
                         .set_threads(args.threads)
                         .set_dict_size(1024*1024*1024)
                         .build_or_load(col);

    LOG(INFO) << "DETERMINE DICT USAGE";
    auto fs = dict_usage_stats(rlz_store);

    s_fs = &fs;
    s_dict = &rlz_store.dict;

    struct mg_mgr mgr;
    struct mg_connection *nc;

    mg_mgr_init(&mgr, NULL);
    nc = mg_bind(&mgr, "1234", ev_handler);
    mg_set_protocol_http_websocket(nc);
    s_http_server_opts.document_root = "../visualize/";

    LOG(INFO) << "Starting HTTP server on port 1234";
    for (;;) {
        mg_mgr_poll(&mgr, 1000);
    }
    mg_mgr_free(&mgr);
    return EXIT_SUCCESS;
}
