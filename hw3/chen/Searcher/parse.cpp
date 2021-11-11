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
    // only consider tokens that starts with an alphabetic letter
    if (isalpha(s[0]) == 0)
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
    uint32_t NUM = 6145;

    // separating characters
    string sep = " ,.;:!@#$%^&*_-=+\\/|{}'\"?()[]";
    const char* csep = sep.c_str();

    // Record time
    auto start = system_clock::now();

    ifstream trec_file("msmarco-docs.trec");
    ofstream pgurl_file("page_url.txt");
    ofstream pgsize_file("page_size.bin", ios::binary);
    vector<int> pgsize;

    // temp variable for reading collection
    string line;
    // Document ID
    uint32_t did = 0;

    // (term, list) mapping
    map<string, vector<uint32_t> > subindex;
    // (DocID, freq) mapping
    map<uint32_t, uint32_t> df;

    uint64_t sizesum = 0;

    while (getline(trec_file, line)) {
        /**
         * Dump into the subindex
         **/
        if (did > 0 && did % NUM == 0 && !subindex.empty()) {
            string subindex_name = "index_n" + to_string(did / NUM);
            cout << subindex_name.c_str() << endl;
            ofstream subindex_file(subindex_name.c_str());

            for (const pair<string, vector<uint32_t> >& n : subindex) {
                subindex_file << n.first.c_str() << ' ';
                df.clear();
                for (auto v : n.second) df[v] += 1;
                for (auto v : df) {
                    subindex_file << v.first  << ',';
                }
                subindex_file << ' ';
                for (auto v : df) {
                    subindex_file << v.second << ',';
                }
                subindex_file << '\n';
            }
            subindex.clear();
            subindex_file.close();

            // Record time
            auto end = system_clock::now();
            auto duration = duration_cast<microseconds>(end - start);
            cout << double(duration.count()) * microseconds::period::num / microseconds::period::den << 's' << endl;

        }
        if (!line.empty()) {
            /** New document **/
            if(line == "<DOC>") {
                for (int j = 0; j < 3; j ++) getline(trec_file, line);

                pgurl_file << line.c_str() << '\n';

                uint32_t size = 0;
                // get and parse content text
                getline(trec_file, line, '<');
                line.erase(remove(line.begin(), line.end(), '\n'), line.end());
                // transform to lower case
                transform(line.begin(), line.end(), line.begin(), ::tolower);
                char* cline = new char[line.length() + 1];
                strcpy(cline, line.c_str());
                char* token = strtok(cline, csep);
                while (token != NULL) {
                    if (check(token)) {
                        subindex[token].push_back(did);
                        size += 1;
                    }
                    token = strtok(NULL, csep);
                }
                delete []cline;

                pgsize.push_back(size);
                
                sizesum += size;
                did ++;
            }
        }
    }
    cout << "Total number of documents: " << did << endl;
    cout << "Average document size:     " << sizesum / did << endl;

    pgsize_file.write(reinterpret_cast<char *>(&pgsize[0]), pgsize.size() * sizeof(pgsize[0]));
    
    // Close files
    pgsize_file.close();
    pgurl_file.close();
    trec_file.close();

    // Record time
    auto end = system_clock::now();
    auto duration = duration_cast<microseconds>(end - start);
    cout << double(duration.count()) * microseconds::period::num / microseconds::period::den << 's' << endl;

    return 0;
}


// 2379.68s
