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
vector<string> split (const string &s, char delim) {
    vector<string> result;
    stringstream ss (s);
    string item;

    while (getline (ss, item, delim)) {
        result.push_back (item);
    }

    return result;
}

/** structure in the priority queue **/
class elem {
    uint32_t fid;
    string token;
    string didlist;
    string freqlist;
public:
    elem (uint32_t _fid, string _posting) {
        fid = _fid;
        vector<string> v = split(_posting, ' ');
        token    = v[0];
        didlist  = v[1];
        freqlist = v[2];
    }
    uint32_t getID() const {return fid;}
    string   getT()  const {return token;}
    string   getDL() const {return didlist;}
    string   getFL() const {return freqlist;}
};

/** comparison operator **/
class comp {
public:
    int operator() (const elem* e1, const elem* e2) {
        if (e1->getT() > e2->getT())
            return true;
        else if (e1->getT() == e2->getT())
            return e1->getID() > e2->getID();
        return false;
    }
};

int main(int argc, char** argv) {

    // Record time
    auto start = system_clock::now();

    uint32_t numfiles = 25;

    cout << "opening files..." << endl;

    for (int j = 0; j < 523; j += 25){
        // cout << j << endl;

        numfiles = (523 - j) > numfiles ? numfiles : (523 - j);

        string line;
        /** priority queue **/
        priority_queue <elem*, vector<elem*>, comp> pq;
        ifstream* group = new ifstream[numfiles];
        ofstream merge("merge_n" + to_string(j / 25 + 1));

        for (int i = 0; i < numfiles; i ++) {
            string index_file = "index_n" + to_string(j + i + 1);
            group[i].open(index_file.c_str());
            /** make sure file open successfully **/
            assert(group[i].is_open());
            getline(group[i], line);
            cout << index_file << endl;
            /** initialize priority queue **/
            pq.push(new elem(i, line));
        }

        assert(pq.size() == numfiles);
        cout << "merging files..." << endl;
        /** while queue is not empty **/
        while (pq.empty() == false) {
            elem*  e        = pq.top();
            string didlist  = e->getDL();
            string freqlist = e->getFL();
            /** A pop is followed by a push **/
            pq.pop();
            if (getline(group[e->getID()], line))
            	pq.push(new elem(e->getID(), line));
            if (pq.empty()) break;
            /** if same token **/
            while ((pq.top())->getT() == e->getT()) {
                elem* ee  = pq.top();
                didlist  += ee->getDL();
                freqlist += ee->getFL();
                pq.pop();
                if (getline(group[ee->getID()], line))
                   pq.push(new elem(ee->getID(), line));
                if (pq.empty()) break;
            }
            merge << e->getT()  << '\n';
            merge << didlist    << '\n';
            merge << freqlist   << '\n';
        }
        cout << "closing files..." << endl;
        for (int i = 0; i < numfiles; i ++)
            group[i].close();
        merge.close();

        // Record time
        auto end = system_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        cout << double(duration.count()) * microseconds::period::num / microseconds::period::den << 's' << endl;

    }

    // Record time
    auto end = system_clock::now();
    auto duration = duration_cast<microseconds>(end - start);
    cout << double(duration.count()) * microseconds::period::num / microseconds::period::den << 's' << endl;

    return 0;
}


// 582.132s
