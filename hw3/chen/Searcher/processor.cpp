#include <cmath>
#include <queue>
#include <vector>
#include <utility>
#include <fstream>
#include <cstring>
#include <sstream>
#include <iostream>
#include <string.h>
#include <algorithm>
#include <mariadb/conncpp.hpp>

using namespace std;
using namespace sql;
using namespace chrono;

// Instantiate Driver
Driver* driver = sql::mariadb::get_driver_instance();
// Configure Connection
SQLString url("jdbc:mariadb://localhost:3306/");
Properties properties({{"user", "root"}, {"password", "123456"}});
// Establish Connection
Connection* conn(driver->connect(url, properties));

#define N 3213835
// 3391243580 / N
// the average length of documents in the collection
#define d_avg 1055

ifstream inverted_index("index_nfinal.bin", ios::binary);
ifstream pgtable("page_table.bin", ios::binary);

/** Helper function for Var-Byte decoding **/
void VBDecode(deque<uint8_t> &dq, vector<uint32_t> &buffer) {
    uint8_t b;
    while (!dq.empty()) {
        uint32_t val = 0, shift = 0;
        b = dq[0];
        while(b < 128) {
            dq.pop_front();
            val += (b << shift);
            shift += 7;
            b = dq[0];
        }
        dq.pop_front();
        val += ((b - 128) << shift);
        buffer.push_back(val);
    }
}

/** Helper function to split a string **/
vector<string> split(const string &s, char delim) {
    vector<string> result;
    stringstream ss(s);
    string item;
    while (getline(ss, item, delim))
        result.push_back(item);
    return result;
}

/** Data structure for search result **/
class result {
    uint32_t did;
public:
    string url;
    double score;
    // term occurrence
    vector<uint32_t> occur1;
    vector<pair<string, uint32_t> > occur2;
    result (uint32_t did, string url, double score, vector<uint32_t> &occurrence) {
        this->did   = did;
        this->url   = url;
        this->score = score;
        this->occur1 = occurrence;
    }
    result (uint32_t did, string url, double score, vector<pair<string, uint32_t> > &occurrence) {
        this->did   = did;
        this->url   = url;
        this->score = score;
        this->occur2 = occurrence;
    }
};
class result_comp {
public:
    bool operator() (const result* r1, const result* r2) {
        return r1->score < r2->score;
    }
};

/** Data structure for lister pointer **/
class list_pointer {
    // TERM of list
    string term;
    // METADATA of list
    vector<uint32_t> lastID;
    vector<uint32_t> chukSZ;
    // start & end of list
    uint64_t start;
    uint64_t end;
    // number of chunks
    int num_of_chks;
    // term frequency, i.e., number of documents that contain term t
    uint32_t f_t;

    /** 
     * CURRENT STATUS in list
     **/
    // current offset in inverted index
    uint64_t offset;
    // lastID from previous chunk
    uint32_t base_did = 0;
    // current did
    uint32_t cdid;
    // current block index
    uint32_t cblock = 0;
    // current did token freq, i.e., frequency of term t in document d
    uint32_t f_dt;
    // current url, associate with current did
    string url;
public:
    list_pointer(string term, uint64_t start, uint64_t end, uint32_t freq) {
        // read from lexicon
        this->term  = term;
        this->start = start;
        this->end   = end;
        this->f_t   = freq;
        inverted_index.seekg(start);
        num_of_chks = ceil(f_t / 128.);
        offset = start + num_of_chks * 8;
        uint32_t tmp;
        for (int i = 0; i < num_of_chks; i ++) {
            inverted_index.read(reinterpret_cast<char *>(&tmp), sizeof(tmp));
            lastID.push_back(tmp);
        }
        for (int i = 0; i < num_of_chks; i ++) {
            inverted_index.read(reinterpret_cast<char *>(&tmp), sizeof(tmp));
            chukSZ.push_back(tmp);
        }
    }

    // disjunctive search only
    int index = 0;
    vector<uint32_t> didlist;
    vector<uint32_t> freqlist;
    vector<double> scorelist;
    vector<string> urllist;
    void get_payload() {
        inverted_index.seekg(offset);
        deque<uint8_t> list;
        for (int i = 0; i < num_of_chks; i ++) {
            for (int j = 0; j < chukSZ[i]; j ++) {
                uint8_t b;
                inverted_index.read(reinterpret_cast<char *>(&b), sizeof(b));
                list.push_back(b);
            }
            vector<uint32_t> buffer;
            VBDecode(list, buffer);
            cdid = base_did; // 0
            for (int i = 0; i < buffer.size() / 2; i ++) {
                cdid += buffer[i];
                didlist.push_back(cdid);
                f_dt = buffer[buffer.size() / 2 + i];
                freqlist.push_back(f_dt);
                scorelist.push_back(get_score());
                urllist.push_back(url);
            }
        }
    }

    int nextGEQ(int k) {
        // block-wise decompression
        while (lastID[cblock] < k) {
            offset  += chukSZ[cblock];
            base_did = lastID[cblock];
            cblock ++;
            if (cblock >= num_of_chks)
                return -1;
        }
        inverted_index.seekg(offset);
        deque<uint8_t> list;
        for (int i = 0; i < chukSZ[cblock]; i ++) {
            uint8_t b;
            inverted_index.read(reinterpret_cast<char *>(&b), sizeof(b));
            list.push_back(b);
        }
        vector<uint32_t> buffer;
        VBDecode(list, buffer);
        cdid = base_did;
        for (int i = 0; i < buffer.size() / 2; i ++) {
            cdid += buffer[i];
            if (cdid >= k) {
                f_dt = buffer[buffer.size() / 2 + i];
                return cdid;
            }
        }
        return -1;
    }

    double get_score() {
        // Seek in page table
        pgtable.seekg(cdid * 1024);
        char *buf = new char[1020];
        pgtable.read(buf, 1020);
        url.assign(buf);
        delete []buf;
        int d;
        pgtable.read(reinterpret_cast<char *>(&d), sizeof(int));

        // BM25
        double K = 1.2 * (0.25 + 0.75 * d / d_avg);
        double score = log((N - f_t + 0.5) / (f_t + 0.5)) * (2.2 * f_dt / (K + f_dt));
        return score;
    }

    string get_term() const { return term; }
    string get_current_url() const { return url; }
    uint32_t get_current_fdt() const { return f_dt; }
    uint32_t get_ft() const { return f_t; }
};
bool list_comp(list_pointer *lp1, list_pointer *lp2) {
    return lp1->get_ft() < lp2->get_ft(); 
}

/** Structure in the priority queue for merge **/
class elem {
    uint32_t lp;
    uint32_t did;
    uint32_t freq;
    double score;
    string url;
public:
    elem (uint32_t lp, uint32_t did, uint32_t freq, double score, string url) {
        this->lp = lp;
        this->did = did;
        this->freq = freq;
        this->score = score;
        this->url = url;
    }
    uint32_t get_lp() const {return lp;}
    uint32_t get_did() const {return did;}
    uint32_t get_freq() const {return freq;}
    double   get_score() const {return score;}
    string   get_url() const {return url;}
};
class comp {
public:
    int operator() (const elem* e1, const elem* e2) {
        return e1->get_did() > e2->get_did();
    }
};

/** Main processor **/
void query_processer(vector<string> &terms, bool conj) {
    /**
     * Priority queue for search results
     **/
    priority_queue <result *, vector<result *>, result_comp> pq;
    /**
     * Get information from Lexicon
     **/
    // Create a new PreparedStatement
    std::unique_ptr<sql::PreparedStatement> stmnt(conn->prepareStatement("SELECT start, end, freq FROM lexicon WHERE term=?"));
    std::unique_ptr<sql::PreparedStatement> snippet(conn->prepareStatement("SELECT text FROM docs WHERE url=?"));
    int num = terms.size();
    vector<list_pointer *> lp;
    for (int i = 0; i < num; i ++) {
        // Bind values to SQL statement
        stmnt->setString(1, terms[i]);
        // Execute query
        ResultSet *res = stmnt->executeQuery();
        // Loop through and print results
        uint64_t start;
        uint64_t end;
        uint32_t freq;
        while (res->next()) {
            start = res->getLong(1);
            end   = res->getLong(2);
            freq  = res->getInt(3);
            lp.push_back(new list_pointer(terms[i], start, end, freq));
        }
    }
    if (lp.empty()) return;

    /**
     * Disjunctive Search
     **/
    if (conj == false) {
        // Merge sorted lists
        priority_queue <elem *, vector<elem *>, comp> merge_list;
        for (int i = 0; i < num; i ++) {
            lp[i]->get_payload();
            merge_list.push(new elem(i, 
                lp[i]->didlist[0],
                lp[i]->freqlist[0],
                lp[i]->scorelist[0],
                lp[i]->urllist[0])
            );
        }
        while (merge_list.empty() == false) {
            elem*  e        = merge_list.top();
            uint32_t ind    = e->get_lp();
            uint32_t did    = e->get_did();
            uint32_t freq   = e->get_freq();
            double score    = e->get_score();
            string url      = e->get_url();
            vector<pair<string, uint32_t> > occurrence;
            occurrence.push_back(make_pair(lp[ind]->get_term(), freq));
            /** A pop is followed by a push **/
            merge_list.pop();
            if (lp[ind]->index < lp[ind]->get_ft() - 1) {
                lp[ind]->index ++;
                merge_list.push(new elem(ind,
                    lp[ind]->didlist[lp[ind]->index],
                    lp[ind]->freqlist[lp[ind]->index],
                    lp[ind]->scorelist[lp[ind]->index],
                    lp[ind]->urllist[lp[ind]->index])
                );
            }
            if (merge_list.empty()) break;
            /** If same docID **/
            while ((merge_list.top())->get_did() == e->get_did()) {
                elem* ee  = merge_list.top();
                ind    = ee->get_lp();
                freq   = ee->get_freq();
                score += ee->get_score();
                occurrence.push_back(make_pair(lp[ind]->get_term(), freq));
                merge_list.pop();
                if (lp[ind]->index < lp[ind]->get_ft() - 1) {
                    lp[ind]->index ++;
                    merge_list.push(new elem(ind,
                        lp[ind]->didlist[lp[ind]->index],
                        lp[ind]->freqlist[lp[ind]->index],
                        lp[ind]->scorelist[lp[ind]->index],
                        lp[ind]->urllist[lp[ind]->index])
                    );
                }
                if (merge_list.empty()) break;
            }
            pq.push(new result(did, url, score, occurrence));
        }
        // Display top-10 search result
        for (int i = 0; i < 10; i ++) {
            if (pq.empty()) break;
            result *r = pq.top();
            cout << "\033[1;31m" << i << "\033[0m: " << r->url << endl;
            // Bind values to SQL statement
            snippet->setString(1, r->url);
            // Execute query
            ResultSet *res = snippet->executeQuery();
            cout << "   SCORE: " << r->score << endl;
            cout << "      " << "TERM" << setw(15) << "FREQUENCY" << endl;
            cout << "      -------------------" << endl;
            for (int j = 0; j < r->occur2.size(); j ++) {
                string term = r->occur2[j].first;
                cout << "      " << term << setw(15 - term.size()) << r->occur2[j].second << endl;
            }
            cout << "      -------------------" << endl;
            while (res->next()) {
                cout << "   SNIPPET: " << res->getString(1).substr(0, 500) << " ......" << endl;
            }
            cout << endl;
            pq.pop();
        }
    }

    /**
     * Conjunctive Search
     **/
    if (conj == true) {
        /* Sort according to list length(freq) */
        sort(lp.begin(), lp.end(), list_comp);
        // for (int i = 0; i < lp.size(); i ++)
        //     cout << lp[i]->get_term() << ' ' << lp[i]->get_ft() << endl;

        int did = 0;
        while (true) {
            // Get next posting from shortest list
            did = lp[0]->nextGEQ(did);
            if (did == -1) break;

            // See if you find entries with same docID in other lists
            int d = -1;
            for (int i = 1; (i < num) && ((d = lp[i]->nextGEQ(did)) == did); i ++) {
            }

            if (d == -1) {
                break;
            } else if (d > did) {
                did = d;
            } else {
                // docID is in intersection; now get all frequencies/scores
                vector<uint32_t> occurrence; 
                double score = 0.0;
                for (int i = 0; i < num; i ++) {
                    score += lp[i]->get_score();
                    occurrence.push_back(lp[i]->get_current_fdt());
                }
                pq.push(new result(did, lp[0]->get_current_url(), score, occurrence));
                did ++;
            }
        }
        // Display top-10 search result
        for (int i = 0; i < 10; i ++) {
            if (pq.empty()) break;
            result *r = pq.top();
            cout << "\033[1;31m" << i << "\033[0m: " << r->url << endl;
            // Bind values to SQL statement
            snippet->setString(1, r->url);
            // Execute query
            ResultSet *res = snippet->executeQuery();
            cout << "   SCORE: " << r->score << endl;
            cout << "      " << "TERM" << setw(15) << "FREQUENCY" << endl;
            cout << "      -------------------" << endl;
            for (int j = 0; j < num; j ++) {
                string term = lp[j]->get_term();
                cout << "      " << term << setw(15 - term.size()) << r->occur1[j] << endl;
            }
            cout << "      -------------------" << endl;
            while (res->next()) {
                cout << "   SNIPPET: " << res->getString(1).substr(0, 500) << " ......" << endl;
            }
            cout << endl;
            pq.pop();
        }
    }
}

int main(int argc, char** argv) {

    bool conj = true;

    // Record time
    auto start = system_clock::now();

    // Create a new Statement
    Statement* select_db(conn->createStatement());
    // Execute query, select database
    select_db->executeQuery("use wse");

    cout << "Welcome to \033[1;31mCoocle\033[0m -- a mini web serach engine" << endl;
    cout << "            Type 'quit' to quit.             " << endl;
    cout << "---------------------------------------------" << endl;
    bool quit = false;
    while (1) {
        string query;
        // Conjunctive or disjunctive
        cout << "Conjunctive search?(y/n/quit)" << endl;
        while (true) {
            cout << ">>> ";
            getline(cin, query);
            if (query == "y") {
                conj = true;
                break;
            } else if (query == "n") {
                conj = false;
                break;
            } else if (query == "quit") {
                quit = true;
                break;
            } else cout << "Not recongized." << endl;
        }
        if (quit == true) {
            cout << "Bye~" << endl;
            break;
        }
        cout << "Please input query..." << endl;
        cout << ">>> ";
        getline(cin, query);
        if (query == "quit") {
            cout << "Bye~" << endl;
            break;
        }
        cout << "Searching..." << endl;
        vector<string> terms = split(query, ' ');

        // Process query
        query_processer(terms, conj);
        cout << "---------------------------------------------" << endl;
    }

    // Close files
    inverted_index.close();
    pgtable.close();

    // Record time
    auto end = system_clock::now();
    auto duration = duration_cast<microseconds>(end - start);
    cout << double(duration.count()) * microseconds::period::num / microseconds::period::den << 's' << endl;

    return 0;
}

