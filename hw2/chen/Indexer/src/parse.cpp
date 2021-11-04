#include <map>
#include <regex>
#include <vector>
#include <utility>
#include <fstream>
#include <cstring>
#include <sstream>
#include <iostream>
#include <string.h>
#include <algorithm>

using namespace std;
using namespace chrono;

bool check(char* s) {
    // limit token length
    if (strlen(s) > 30)
        return false;
    // only consider tokens that only consists of english letter and numbers
    for (int i = 0; i < strlen(s); i ++) {
        if (isalnum(s[i]) == 0)
            return false;
    }
    return true;
}

int main(int argc, char** argv) {

    // number of documents per subindex
    uint32_t NUM = atoi(argv[1]);

    // seperating characters
    string sep = " ,.;:!@#$%^&*_-=+\\/|{}'\"?()[]";
    const char* csep = sep.c_str();

    // Record time
    auto start = system_clock::now();

    ifstream trec("msmarco-docs.trec");
    ofstream pgtable("page_table.bin", ios::binary);

    string line;
    // Document ID
    uint32_t did = 0;

    char page_url[NUM][1020];
    uint32_t page_size[NUM];

    // (term, list) mapping
    map<string, vector<uint32_t> > inverted_index;
    // (DocID, freq) mapping
    map<int, int> df;

    while (getline(trec, line)) {
        if (did > 0 && did % NUM == 0 && !inverted_index.empty()) {
            /**
             * Dump into the subindex
             **/
            // cout << inverted_index.size() << endl;
            for (int i = 0; i < NUM; i ++) {
                pgtable.write(page_url[i], 1020);
                pgtable.write((const char *)&page_size[i], 4);
            }
            string index_file = "index_n" + to_string(did / NUM);
            cout << index_file.c_str() << endl;
            ofstream indexer(index_file.c_str());

            for (const pair<string, vector<uint32_t> >& n : inverted_index) {
                indexer << n.first.c_str() << ' ';
                df.clear();
                for (auto v : n.second) df[v] += 1;
                for (auto v : df) {
                    indexer << v.first  << ',';
                }
                indexer << ' ';
                for (auto v : df) {
                    indexer << v.second << ',';
                }
                indexer << '\n';
            }
            indexer.close();
            inverted_index.clear();

            // Record time
            auto end = system_clock::now();
            auto duration = duration_cast<microseconds>(end - start);
            cout << double(duration.count()) * microseconds::period::num / microseconds::period::den << 's' << endl;

        }
        if (!line.empty()) {
            if(line == "<DOC>") {
                for (int j=0;j<3;j++) getline(trec, line);

                strcpy(page_url[did % NUM], line.c_str());

                uint32_t size = 0;
                // get and parse content text
                getline(trec, line, '<');
                line.erase(remove(line.begin(), line.end(), '\n'), line.end());
                char* cline = new char[line.length() + 1];
                strcpy(cline, line.c_str());
                char* token = strtok(cline, csep);
                while (token != NULL) {
                    if (check(token)) {
                        inverted_index[token].push_back(did);
                    }
                    token = strtok(NULL, csep);
                }
                delete []cline;

                page_size[did % NUM] = size;
                did ++;
            }
        }
    }

    pgtable.close();
    trec.close();

    // Record time
    auto end = system_clock::now();
    auto duration = duration_cast<microseconds>(end - start);
    cout << double(duration.count()) * microseconds::period::num / microseconds::period::den << 's' << endl;

    return 0;
}

