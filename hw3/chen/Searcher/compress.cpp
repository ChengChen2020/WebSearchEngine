#include <map>
#include <queue>
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

/** helper function to split a string **/
vector<uint32_t> split (const string &s, char delim) {
    vector<uint32_t> result;
    stringstream ss (s);
    string item;

    while (getline (ss, item, delim)) {
        result.push_back (stoi(item));
    }

    return result;
}

vector<uint8_t> buffer;

/** var byte encoding **/
void VBEncode(uint32_t num){
    while (num > 127) {
        buffer.push_back(num & 127);
        num >>= 7;
    }
    buffer.push_back(num + 128);
}

int main(int argc, char** argv) {

    // Record time
    auto start = system_clock::now();

    cout << "opening files..." << endl;

    ifstream ascii_file("merge_nfinal");
    ofstream bin_file("index_nfinal.bin", ios::binary);
    ofstream lexicon_file("lexicon.txt");

    /** make sure file open successfully **/
    assert(ascii_file.is_open());
    assert(bin_file.is_open());
    assert(lexicon_file.is_open());

    cout << "opening succeed" << endl;
    string token;
    string didlist;
    string freqlist;
    uint64_t list_start = 0;
    uint64_t token_cnt = 0;

    while (getline(ascii_file, token)) {

        token_cnt ++;
        if (token_cnt % 1000000 == 0)
            cout << token_cnt << endl;

        getline(ascii_file, didlist);
        getline(ascii_file, freqlist);

        vector<uint32_t> d = split(didlist , ',');
        vector<uint32_t> f = split(freqlist, ',');

        assert(d.size() == f.size());

        uint32_t num_of_dids = d.size();
        uint32_t prev = 0;
        uint64_t list_size = 0;
        /** record metadata **/
        vector<uint32_t> meta_lastID;
        vector<uint32_t> meta_chukSZ;

        buffer.clear();
        /** chunk size 128 **/
        for (int i = 0; i < num_of_dids; i += 128) {
            int cnt = num_of_dids - i < 128 ? num_of_dids - i : 128;
            for (int j = 0; j < cnt; j ++) {
                /** encode difference **/
                VBEncode(d[i + j] - prev);
                prev = d[i + j];
            }
            for (int j = 0; j < cnt; j ++)
                VBEncode(f[i + j]);
            /** record metadata **/
            meta_lastID.push_back(d[i + cnt - 1]);
            meta_chukSZ.push_back(buffer.size() - list_size);
            list_size = buffer.size();
        }

        /** write to file **/
        bin_file.write(reinterpret_cast<const char*>(&meta_lastID[0]), meta_lastID.size()*sizeof(uint32_t));
        bin_file.write(reinterpret_cast<const char*>(&meta_chukSZ[0]), meta_chukSZ.size()*sizeof(uint32_t));
        bin_file.write(reinterpret_cast<const char*>(&buffer[0]),      buffer.size()*sizeof(uint8_t));

        list_size += (meta_lastID.size()*sizeof(uint32_t) + meta_chukSZ.size()*sizeof(uint32_t));

        lexicon_file << token << ' ' << list_start << ' ' << list_start + list_size << ' ' << num_of_dids << endl;

        list_start += list_size;
    }

    ascii_file.close();
    bin_file.close();

    // Record time
    auto end = system_clock::now();
    auto duration = duration_cast<microseconds>(end - start);
    cout << double(duration.count()) * microseconds::period::num / microseconds::period::den << 's' << endl;

    return 0;
}


// 443.587s

