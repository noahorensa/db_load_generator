#include <mysql_database.h>
#include <csv.h>
#include <string.h>
#include <iostream>
#include <file.h>
#include <thread_pool.h>
#include <mutex>
#include <sync_condition.h>
#include <chrono>

#define MB ((size_t) (1024 * 1024))

using namespace spl;

enum class DB {
    MYSQL,
};

static struct {
    DB dbType = DB::MYSQL;

    const char *host = nullptr;
    const char *user = nullptr;
    const char *password = nullptr;
    const char *database = nullptr;
    int port = 0;

    const char *table = nullptr;

    size_t threads = 1;

    size_t maxMemory = 128 * MB;

    bool loadCsv = false;
    const char *csvPath = nullptr;
    CSVOptions *csvOptions = nullptr;

    bool runQueries = false;
    const char *queryPath = nullptr;
    const char *queryStatPath = "result";
} args;

static std::mutex _connectionsMtx;
static std::vector<Database *> connections;
static thread_local Database *db;

bool parseArguments(int argc, char **argv) {
    for (int i = 0; i < argc; ++i) {
        if (strcmp(argv[i], "--db") == 0) {
            ++i;
            if (i == argc) return false;
            if (strcmp(argv[i], "mysql") == 0) {
                args.dbType = DB::MYSQL;
            }
            else {
                std::cerr << "Unsupported database type '" << argv[i] << "'\n";
                return false;
            }
        }
        else if (strcmp(argv[i], "--host") == 0) {
            ++i;
            if (i == argc) return false;
            args.host = argv[i];
        }
        else if (strcmp(argv[i], "--user") == 0) {
            ++i;
            if (i == argc) return false;
            args.user = argv[i];
        }
        else if (strcmp(argv[i], "--password") == 0) {
            ++i;
            if (i == argc) return false;
            args.password = argv[i];
        }
        else if (strcmp(argv[i], "--database") == 0) {
            ++i;
            if (i == argc) return false;
            args.database = argv[i];
        }
        else if (strcmp(argv[i], "--port") == 0) {
            ++i;
            if (i == argc) return false;
            args.port = atoi(argv[i]);
        }
        else if (strcmp(argv[i], "--table") == 0) {
            ++i;
            if (i == argc) return false;
            args.table = argv[i];
        }
        else if (strcmp(argv[i], "--threads") == 0) {
            ++i;
            if (i == argc) return false;
            args.threads = atoi(argv[i]);
        }
        else if (strcmp(argv[i], "--memory") == 0) {
            ++i;
            if (i == argc) return false;
            args.maxMemory = (size_t) atoi(argv[i]) * MB;
        }
        else if (strcmp(argv[i], "--load-csv") == 0) {
            args.loadCsv = true;

            ++i;
            if (i == argc) return false;
            auto opt = strdup(argv[i]);

            auto p = strtok(opt, ":");
            if (p == nullptr)  {
                std::cerr << "Invalid option '" << argv[i] << "' for --load-csv\n";
                return false;
            }
            args.csvPath = p;

            p = strtok(nullptr, ",");
            std::vector<CSVField> fields;
            while (p != nullptr) {

                if (strncmp(p, "uint8", 5) == 0) {
                    fields.push_back(CSVField(DataType::UINT8));
                }
                else if (strncmp(p, "uint16", 6) == 0) {
                    fields.push_back(CSVField(DataType::UINT16));
                }
                else if (strncmp(p, "uint32", 6) == 0) {
                    fields.push_back(CSVField(DataType::UINT32));
                }
                else if (strncmp(p, "uint64", 6) == 0) {
                    fields.push_back(CSVField(DataType::UINT64));
                }
                else if (strncmp(p, "int8", 4) == 0) {
                    fields.push_back(CSVField(DataType::INT8));
                }
                else if (strncmp(p, "int16", 5) == 0) {
                    fields.push_back(CSVField(DataType::INT16));
                }
                else if (strncmp(p, "int32", 5) == 0) {
                    fields.push_back(CSVField(DataType::INT32));
                }
                else if (strncmp(p, "int64", 5) == 0) {
                    fields.push_back(CSVField(DataType::INT64));
                }
                else if (strncmp(p, "float32", 7) == 0) {
                    fields.push_back(CSVField(DataType::FLOAT32));
                }
                else if (strncmp(p, "float64", 7) == 0) {
                    fields.push_back(CSVField(DataType::FLOAT64));
                }
                else if (strncmp(p, "string", 6) == 0) {
                    p += 6;
                    if (*p != '(') {
                        std::cerr << "Unexpected token in options for --load-csv\n";
                        return false;
                    }
                    ++p;
                    char *end = p;
                    while (*end != '\0' && *end != ')') ++end;
                    if (*end != ')') {
                        std::cerr << "Unexpected token in options for --load-csv\n";
                        return false;
                    }
                    *end = '\0';
                    fields.push_back(CSVField(DataType::STRING, atoi(p)));
                }
                else if (strncmp(p, "mysql_date", 7) == 0) {
                    fields.push_back(CSVField(DataType::MYSQL_DATE));
                }
                else {
                    std::cerr << "Invalid CSV column type '" << p << "'\n";
                    return false;
                }

                p = strtok(nullptr, ",");
            }

            args.csvOptions = new CSVOptions(fields);
        }
        else if (strcmp(argv[i], "--csv-delimiter") == 0) {
            if (! args.loadCsv) {
                std::cerr << "Option --csv-delimiter must follow a --load-csv option\n";
                return false;
            }

            ++i;
            if (i == argc) return false;
            args.csvOptions->delimiter = argv[i][0];
        }
        else if (strcmp(argv[i], "--no-csv-header") == 0) {
            if (! args.loadCsv) {
                std::cerr << "Option --no-csv-header must follow a --load-csv option\n";
                return false;
            }

            args.csvOptions->header = false;
        }
        else if (strcmp(argv[i], "--run") == 0) {
            ++i;
            if (i == argc) return false;
            args.runQueries = true;
            args.queryPath = argv[i];
        }
        else if (strcmp(argv[i], "--results-file") == 0) {
            if (! args.runQueries) {
                std::cerr << "Option --results-file must follow a --run option\n";
                return false;
            }

            ++i;
            if (i == argc) return false;
            args.queryStatPath = argv[i];
        }
        else {
            std::cerr << "Unknown option '" << argv[i] << "'\n";
            return false;
        }
    }

    if (args.host == nullptr) {
        std::cerr << "No database host specified\n";
        return false;
    }
    if (args.user == nullptr) {
        std::cerr << "No database user specified\n";
        return false;
    }
    if (args.password == nullptr) {
        std::cerr << "No database password specified\n";
        return false;
    }
    if (args.database == nullptr) {
        std::cerr << "No database schema specified\n";
        return false;
    }
    if (args.loadCsv && args.table == nullptr) {
        std::cerr << "No table specified for --load-csv\n";
        return false;
    }

    return true;
}

void instantiateDB() {
    if (db != nullptr) return;

    switch (args.dbType) {
    case DB::MYSQL: {
        db = new MySQLDatabase(
            args.host,
            args.user,
            args.password,
            args.database,
            args.port
        );
        std::unique_lock lk(_connectionsMtx);
        connections.push_back(db);
    }
    break;

    default: break;
    }
}

void closeAllConnections() {
    for (auto c : connections) delete c;
    connections.clear();
}

void loadCsvData() {

    std::cout << "Preparing to load CSV data into table '" << args.table << "'\n";

    ThreadPool pool(args.threads);
    SynchronizationCondition tasks;
    SynchronizationCondition memory(args.maxMemory);

    auto files = File::list(args.csvPath);

    auto start = std::chrono::high_resolution_clock::now();

    for (const auto &p : files) {
        memory.wait();
        PathInfo pInfo(p);
        memory.increase(pInfo.length());

        std::cout << "Reading file " << p.get() << '\n';

        for (auto chunk : CSV::read(p.get(), *args.csvOptions)) {
            tasks.increase(1);
            memory.increase(chunk->memorySize());
            pool.run([chunk, &tasks, &memory] (auto) {
                try {
                    instantiateDB();

                    std::cout << "Loading data chunk ("
                        << chunk->size() << " rows) into table '"
                        << args.table << "'\n";

                    db->loadIntoTable(args.table, chunk);
                }
                catch (const std::exception &e) {
                    std::cerr << e.what() << "\n";
                }
                catch (...) {
                    std::cerr << "An unknown exception occurred while loading CSV file\n";
                }

                memory.decrease(chunk->memorySize());
                delete chunk;
                tasks.decrease(1);
            });
        }

        memory.decrease(pInfo.length());
    }

    tasks.wait();
    pool.terminate();
    closeAllConnections();

    auto loadEnd = std::chrono::high_resolution_clock::now();

    std::cout << "Finished data loading in " << (loadEnd - start).count() / 1e9 << "\n";
}

List<std::string> * readQueries(const Path &path) {
    auto queries = new List<std::string>();
    
    File f(path);
    auto buf = new char[f.info().length() + 1];
    f.read(buf, f.info().length());

    std::stringstream s;

    auto p = buf;
    while (*p != '\0') {
        // ignore empty lines
        while (*p == '\n') ++p;

        // ignore lines starting with --
        if (strncmp(p, "--", 2) == 0) {
            while (*p != '\0' && *p != '\n') ++p;
            continue;
        }

        while (*p != '\0' && *p != '\n' && *p != ';') {
            s << *p;
            ++p;
        }
        if (*p == '\n') {
            s << *p;
            ++p;
        }
        else if (*p == ';') {
            ++p;
            queries->append(s.str());
            s = std::stringstream();
        }
    }

    delete[] buf;

    return queries;
}

void runQueries() {
    std::cout << "Preparing to run benchmark queries\n";

    std::atomic<size_t> queryCount = 0;

    auto files = File::list(args.queryPath);

    List<List<std::string> *> streams;

    for (const auto &p : files) {
        std::cout << "Reading query file " << p.get() << "\n";

        auto queries = readQueries(p);
        queryCount += queries->size();
        streams.append(queries);
    }

    std::cout << "Running " << streams.size() << " query streams using " << args.threads << " threads\n";

    ThreadPool pool(args.threads);
    SynchronizationCondition tasks;

    auto start = std::chrono::high_resolution_clock::now();

    size_t streamIndex = 0;
    for (auto s : streams) {
        tasks.increase(1);
        pool.run([streamIndex, s, &tasks, &queryCount] (auto) {
            std::cout << "Running query stream " << streamIndex << "\n";

            try {
                instantiateDB();
            }
            catch (const std::exception &e) {
                std::cerr << e.what() << "\n";
                delete s;
                tasks.decrease(1);
                return;
            }
            catch (...) {
                std::cerr << "An unknown exception occurred while loading CSV file\n";
                delete s;
                tasks.decrease(1);
                return;
            }

            for (const auto &q : *s) {
                try {
                    db->query(q);
                }
                catch (const std::exception &e) {
                    std::cerr << e.what() << "\n";
                    --queryCount;
                }
                catch (...) {
                    std::cerr << "An unknown exception occurred while loading CSV file\n";
                    --queryCount;
                }
            }

            delete s;
            tasks.decrease(1);
        });

        ++streamIndex;
    }

    tasks.wait();
    auto end = std::chrono::high_resolution_clock::now();

    closeAllConnections();
    pool.terminate();

    double queryTime = (end - start).count() / 1e9;

    std::cout << "Finished " << queryCount
        << " queries in " << queryTime
        << " seconds\n";

    std::stringstream stat;
    stat << queryCount << ',' << queryTime;
    auto statStr = stat.str();
    File statFile(args.queryStatPath);
    statFile.open(File::READ_WRITE | File::CREATE | File::TRUNCATE);
    statFile.write(statStr.data(), statStr.size());
}

int main(int argc, char **argv) {

    if (! parseArguments(argc - 1, argv + 1)) exit(1);

    if (args.loadCsv) loadCsvData();
    if (args.runQueries) runQueries();

    exit(0);
}
