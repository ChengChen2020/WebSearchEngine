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

// pointer to page table
std::unique_ptr<sql::PreparedStatement> urldb(conn->prepareStatement("SELECT url FROM pgurl WHERE id=?"));

#define N 3213835
// 3192517977 / N
// the average length of documents in the collection
#define d_avg 993

vector<uint32_t> pgsize(N, 0);

ifstream inverted_index_file("index_nfinal.bin", ios::binary);
ifstream pgsize_file("page_size.bin", ios::binary);

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
public:
    uint32_t did;
    double score;
    // term occurrence
    vector<uint32_t> occur1;
    vector<pair<string, uint32_t> > occur2;
    // term score
    vector<double> ts1;
    vector<pair<string, double> > ts2;
    result (uint32_t did, double score, 
        vector<uint32_t> &occurrence, vector<double> &term_score) {
        this->did    = did;
        this->score  = score;
        this->occur1 = occurrence;
        this->ts1    = term_score;
    }
    result (uint32_t did, double score, 
        vector<pair<string, uint32_t> > &occurrence,
        vector<pair<string, double> > &term_score) {
        this->did    = did;
        this->score  = score;
        this->occur2 = occurrence;
        this->ts2    = term_score;
    }
};
class result_comp {
public:
    bool operator() (const result* r1, const result* r2) {
        return r1->score > r2->score;
    }
};

/** Data structure for lister pointer **/
class list_pointer {
    // TERM of list
    string term;
    // METADATA of list
    vector<uint32_t> lastID;
    vector<uint32_t> chukSZ;
    // list content (compressed)
    vector<char> compressed_list;
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
    // current offset in inverted list
    uint64_t offset = 0;
    // lastID from previous chunk
    uint32_t base_did = 0;
    // current did
    uint32_t cdid;
    // current block index
    uint32_t cblock = 0;
    // current did token freq, i.e., frequency of term t in document d
    uint32_t f_dt;
public:
    list_pointer(string term, uint64_t start, uint64_t end, uint32_t freq) {
        // read from lexicon
        this->term  = term;
        this->start = start;
        this->end   = end;
        this->f_t   = freq;
        
        inverted_index_file.seekg(start);
        num_of_chks = ceil(f_t / 128.);
        
        lastID.assign(num_of_chks, 0);
        inverted_index_file.read(reinterpret_cast<char *>(&lastID[0]), num_of_chks*sizeof(lastID[0]));
        
        chukSZ.assign(num_of_chks, 0);
        inverted_index_file.read(reinterpret_cast<char *>(&chukSZ[0]), num_of_chks*sizeof(chukSZ[0]));

        // list content (compressed)
        compressed_list.assign(end - start, 0);
        inverted_index_file.read(reinterpret_cast<char *>(&compressed_list[0]), compressed_list.size());
    }

    // disjunctive search only
    int index = 0;
    vector<uint32_t> didlist;
    vector<uint32_t> freqlist;
    vector<double>   scorelist;
    void get_payload() {
        for (int i = 0; i < num_of_chks; i ++) {
            deque<uint8_t> compressed_block(compressed_list.begin()+offset, compressed_list.begin()+offset+chukSZ[i]);
            vector<uint32_t> decoded_block;
            VBDecode(compressed_block, decoded_block);
            cdid = base_did; // 0
            for (int i = 0; i < decoded_block.size() / 2; i ++) {
                cdid += decoded_block[i];
                didlist.push_back(cdid);
                f_dt  = decoded_block[decoded_block.size() / 2 + i];
                freqlist.push_back(f_dt);
                scorelist.push_back(get_score());
            }
            base_did = cdid;
            offset += chukSZ[i];
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
        deque<uint8_t> compressed_block(compressed_list.begin()+offset, compressed_list.begin()+offset+chukSZ[cblock]);
        vector<uint32_t> decoded_block;
        VBDecode(compressed_block, decoded_block);
        cdid = base_did;
        for (int i = 0; i < decoded_block.size() / 2; i ++) {
            cdid += decoded_block[i];
            if (cdid >= k) {
                f_dt = decoded_block[decoded_block.size() / 2 + i];
                return cdid;
            }
        }
        return -1;
    }

    double get_score() {
        double K = 1.2 * (0.25 + 0.75 * pgsize[cdid] / d_avg);
        return log((N - f_t + 0.5) / (f_t + 0.5)) * (2.2 * f_dt / (K + f_dt));
    }

    string get_term() const { return term; }
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
    double   score;
public:
    elem (uint32_t lp, uint32_t did, uint32_t freq, double score) {
        this->lp = lp;
        this->did = did;
        this->freq = freq;
        this->score = score;
    }
    uint32_t get_lp() const {return lp;}
    uint32_t get_did() const {return did;}
    uint32_t get_freq() const {return freq;}
    double   get_score() const {return score;}
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
    std::unique_ptr<sql::PreparedStatement> lexicondb(conn->prepareStatement("SELECT start, end, freq FROM lexicon WHERE term=?"));
    std::unique_ptr<sql::PreparedStatement> contentdb(conn->prepareStatement("SELECT text FROM docs WHERE url=?"));
    int num = terms.size();
    vector<list_pointer *> lp;
    for (int i = 0; i < num; i ++) {
        // Bind values to SQL statement
        lexicondb->setString(1, terms[i]);
        // Execute query
        ResultSet *res = lexicondb->executeQuery();
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

    cout << "--------------------" << endl;
    cout << "INVERTED LIST LENGTH" << endl;
    for (int i = 0; i < lp.size(); i ++) {
        cout << lp[i]->get_term() << setw(20 - lp[i]->get_term().size()) << lp[i]->get_ft() << endl;
    }
    cout << "--------------------" << endl;

    /**
     * Disjunctive Search
     **/
    if (conj == false) {
        // Merge sorted lists

        priority_queue <elem *, vector<elem *>, comp> merge_buffer;
        for (int i = 0; i < num; i ++) {
            lp[i]->get_payload();
            merge_buffer.push(new elem(i, 
                lp[i]->didlist[0],
                lp[i]->freqlist[0],
                lp[i]->scorelist[0])
            );
        }
        while (merge_buffer.empty() == false) {
            elem*  e        = merge_buffer.top();
            uint32_t ind    = e->get_lp();
            uint32_t did    = e->get_did();
            uint32_t freq   = e->get_freq();
            double score    = e->get_score();
            vector<pair<string, uint32_t> > occurrence;
            vector<pair<string, double> >   term_score;
            occurrence.push_back(make_pair(lp[ind]->get_term(), freq));
            term_score.push_back(make_pair(lp[ind]->get_term(), score));
            /** A pop is followed by a push **/
            merge_buffer.pop();
            if (lp[ind]->index < lp[ind]->get_ft() - 1) {
                lp[ind]->index ++;
                merge_buffer.push(new elem(ind,
                    lp[ind]->didlist[lp[ind]->index],
                    lp[ind]->freqlist[lp[ind]->index],
                    lp[ind]->scorelist[lp[ind]->index])
                );
            }
            if (merge_buffer.empty()) break;
            /** If same docID **/
            while ((merge_buffer.top())->get_did() == e->get_did()) {
                elem* ee  = merge_buffer.top();
                ind    = ee->get_lp();
                freq   = ee->get_freq();
                score += ee->get_score();
                occurrence.push_back(make_pair(lp[ind]->get_term(), freq));
                term_score.push_back(make_pair(lp[ind]->get_term(), ee->get_score()));
                merge_buffer.pop();
                if (lp[ind]->index < lp[ind]->get_ft() - 1) {
                    lp[ind]->index ++;
                    merge_buffer.push(new elem(ind,
                        lp[ind]->didlist[lp[ind]->index],
                        lp[ind]->freqlist[lp[ind]->index],
                        lp[ind]->scorelist[lp[ind]->index])
                    );
                }
                if (merge_buffer.empty()) break;
            }
            if (pq.size() >= 10) {
                if ((pq.top())->score < score) {
                    pq.pop();
                    pq.push(new result(did, score, occurrence, term_score));
                }
            } else {
                pq.push(new result(did, score, occurrence, term_score));
            }
        }

        // Display top-10 search result
        string display[10];
        for (int i = 0; i < 10; i ++) {
            if (pq.empty()) break;
            result *r = pq.top();
            // Bind values to SQL statement
            urldb->setInt(1, r->did);
            // Execute query
            ResultSet *res = urldb->executeQuery();
            string url;
            while (res->next()) {
                url = res->getString(1);
            }
            display[i] += "\033[1;31m" + to_string(10 - i) + "\033[0m: " + url + "\n";
            display[i] += "   SCORE: " + to_string(r->score) + "\n";
            display[i] += "      TERM          FREQUENCY\n";
            display[i] += "      -----------------------\n";
            for (int j = 0; j < r->occur2.size(); j ++) {
                string term = r->occur2[j].first;
                display[i] += "      " + term + string(23 - term.size() - to_string(r->occur2[j].second).size(), ' ') + to_string(r->occur2[j].second) + "\n";
            }
            display[i] += "      -----------------------\n";
            display[i] += "      TERM              SCORE\n";
            display[i] += "      -----------------------\n";
            for (int j = 0; j < r->ts2.size(); j ++) {
                string term = r->ts2[j].first;
                display[i] += "      " + term + string(23 - term.size() - to_string(r->ts2[j].second).size(), ' ') + to_string(r->ts2[j].second) + "\n";
            }
            display[i] += "      -----------------------\n";

            // Bind values to SQL statement
            contentdb->setString(1, url);
            // Execute query
            res = contentdb->executeQuery();
            while (res->next()) {
                display[i] += "   SNIPPET: " + res->getString(1).substr(0, 500) + " ......\n";
            }
            display[i] += "\n";
            pq.pop();
        }
        for (int i = 9; i >= 0; i --) {
            cout << display[i];
        }
    }

    /**
     * Conjunctive Search
     **/
    if (conj == true) {
        /* Sort according to list length */
        sort(lp.begin(), lp.end(), list_comp);

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
                vector<double> term_score;
                double score = 0.0;
                for (int i = 0; i < num; i ++) {
                    score += lp[i]->get_score();
                    occurrence.push_back(lp[i]->get_current_fdt());
                    term_score.push_back(lp[i]->get_score());
                }
                if (pq.size() >= 10) {
                    if ((pq.top())->score < score) {
                        pq.pop();
                        pq.push(new result(did, score, occurrence, term_score));
                    }
                } else {
                    pq.push(new result(did, score, occurrence, term_score));
                }
                did ++;
            }
        }

        // Display top-10 search result
        string display[10];
        for (int i = 0; i < 10; i ++) {
            if (pq.empty()) break;
            result *r = pq.top();
            // Bind values to SQL statement
            urldb->setInt(1, r->did);
            // Execute query
            ResultSet *res = urldb->executeQuery();
            string url;
            while (res->next()) {
                url = res->getString(1);
            }
            display[i] += "\033[1;31m" + to_string(10 - i) + "\033[0m: " + url + "\n";
            display[i] += "   SCORE: " + to_string(r->score) + "\n";
            display[i] += "      TERM          FREQUENCY\n";
            display[i] += "      -----------------------\n";
            for (int j = 0; j < num; j ++) {
                string term = lp[j]->get_term();
                display[i] += "      " + term + string(23 - term.size() - to_string(r->occur1[j]).size(), ' ') + to_string(r->occur1[j]) + "\n";
            }
            display[i] += "      -----------------------\n";
            display[i] += "      TERM              SCORE\n";
            display[i] += "      -----------------------\n";
            for (int j = 0; j < num; j ++) {
                string term = lp[j]->get_term();
                display[i] += "      " + term + string(23 - term.size() - to_string(r->ts1[j]).size(), ' ') + to_string(r->ts1[j]) + "\n";
            }
            display[i] += "      -----------------------\n";

            // Bind values to SQL statement
            contentdb->setString(1, url);
            // Execute query
            res = contentdb->executeQuery();
            while (res->next()) {
                display[i] += "   SNIPPET: " + res->getString(1).substr(0, 500) + " ......\n";
            }
            display[i] += "\n";
            pq.pop();
        }
        for (int i = 9; i >= 0; i --) {
            cout << display[i];
        }
    }
}

int main(int argc, char** argv) {

    bool conj = true;

    // Create a new Statement
    Statement* select_db(conn->createStatement());
    // Execute query, select database
    select_db->executeQuery("use wse");

    cout << "Welcome to \033[1;31mCoocle\033[0m -- a mini web serach engine" << endl;
    cout << "            Type 'quit' to quit.             " << endl;
    cout << "N     = 3213835" << endl;
    cout << "d_avg =     993" << endl;
    cout << "---------------------------------------------" << endl;
    cout << "Lexicon is stored in database...             " << endl;
    cout << "Loading page table..............             " << endl;
    pgsize_file.read(reinterpret_cast<char *>(&pgsize[0]), N * sizeof(pgsize[0]));
    cout << "Done..............                           " << endl;
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

        // Record time
        auto start = system_clock::now();
        // Process query
        query_processer(terms, conj);
        // Record time
        auto end = system_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        cout << double(duration.count()) * microseconds::period::num / microseconds::period::den << 's' << endl;

        cout << "---------------------------------------------" << endl;
    }

    // Close files
    inverted_index_file.close();
    pgsize_file.close();

    return 0;
}

