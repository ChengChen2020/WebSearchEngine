Follow https://github.com/Nv7-GitHub/googlesearch to install googlesearch locally.

The structure is as follows:

- Chen
    - readme.txt
    - explain.txt
    - Crawler
        - .git/ (for github)
        - .gitignore (for github)
        - README.md (for github)

        - crawler.py (src for crawler with priority score, `python crawler.py -h` for usage)
        - naivecrawler.py (src for crawler using simple BFS, `python naivecrawler.py -h` for usage)

        - log_brooklyn union.csv (10000 pages for query 'brooklyn union' with priority) (Average rate 6 p/s)
        - log_paris texas.csv (10000 pages for query 'paris texas' with priority) (Average rate 4.5 p/s)

        - log_naive_brooklyn union.csv (10000 pages for query 'brooklyn union' with no priority) (Average rate 11 p/s)
        - log_naive_paris texas.csv (10000 pages for query 'paris texas' with no priority)
