/*
    B18048 - Ashwin Ginoria
    B18092 - Vivek Kumar
    B18093 - Vyom Goel
    B18149 - Vasu Gupta
*/

#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <fstream>
#include <vector>
#include <map>
#include <set>
#include <mutex>
#include <signal.h>

using namespace std;

mutex mu;

class Transaction {
public:
    // ID_Format = Tx012
    string ID;

    // list of Operations
    vector <string> operations;
    
    // final outcome i.e. commited or aborted
    bool outcome;
};

class LockMgr {
    // Lock : <variableName, TransactionID>
    // Default Value is assigned if variable in Question is unlocked
    // condition_var : <variableName,conditionVariable>

protected:
    pthread_mutex_t lock;
    map <string, pthread_cond_t> condition_var;
    map <string, set<string>> readLock;
    map <string, string> writeLock;

    // Constructor to intitialize readLock, writeLock, lock and conditon variables
    // arg_variables: <variable_name, its_initial_value>
    LockMgr (vector <pair<string, int> > arg_variables) {
        lock=PTHREAD_MUTEX_INITIALIZER;

        for (auto it:arg_variables) {
            condition_var[it.first]=PTHREAD_COND_INITIALIZER;
        }
    }

    void acquireReadLock (string TransactionID, string variableName){
        // mutex lock to avoid simulataneous manipulation of condition variable
        pthread_mutex_lock(&lock);

        // check if there is already a writeLock on that variable by some other thread
        while(writeLock.find(variableName)!=writeLock.end()) {
            mu.lock();
            cout << "Wait_R-Lock [" << TransactionID << ", " << variableName << "]\n";
            mu.unlock();
            pthread_cond_wait(&condition_var[variableName], &lock);
        }
        
        // insert the transaction id into the set for that variable
        readLock[variableName].insert(TransactionID);

        mu.lock();
        cout << "R-Lock [" << TransactionID << ", " << variableName << "]\n";
        mu.unlock();

        // unlocking mutex
        pthread_mutex_unlock(&lock);
    }

    void acquireWriteLock (string TransactionID, string variableName){
        // mutex lock to avoid simulataneous manipulation of condition variable
        pthread_mutex_lock(&lock);

        // check if there is already a writeLock or a readLock on that variable by some other thread
        while(writeLock.find(variableName)!=writeLock.end() || readLock[variableName].size()>0) {
            mu.lock();
            cout << "Wait_W-Lock [" << TransactionID << ", " << variableName << "]\n";
            mu.unlock();
            pthread_cond_wait(&condition_var[variableName], &lock);
        }

        mu.lock();
        cout << "W-Lock [" << TransactionID << ", " << variableName << "]\n";
        mu.unlock();
        
        writeLock[variableName]=TransactionID;
        // unlocking mutex
        pthread_mutex_unlock(&lock);
    }

    void upgradeLock (string TransactionID, string variableName){
        // mutex lock to avoid simulataneous manipulation of condition variable
        pthread_mutex_lock(&lock);

        // if some other thread is reading or writing this variable, put this thread on sleep
        while (readLock[variableName].size()>1 || writeLock.find(variableName)!=writeLock.end()) {    
            mu.lock();
            cout << "Wait_Upgrade [" << TransactionID << ", " << variableName << "]\n";
            mu.unlock();
      
            pthread_cond_wait(&condition_var[variableName], &lock);
        }

        // Clearing readlock 
        readLock[variableName].clear();

        // Adding TID to writelog
        writeLock[variableName]=TransactionID;

        mu.lock();
        cout << "Upgrade [" << TransactionID << ", " << variableName << "]\n";
        mu.unlock();
        
        // unlocking mutex
        pthread_mutex_unlock(&lock);
    }

    void releaseLock (string TransactionID){
        // mutex lock to avoid simulataneous manipulation of condition variable
        pthread_mutex_lock(&lock);

        // release the readLocks of this transaction
        for(auto &it:readLock){
            if(it.second.find(TransactionID)!=it.second.end()){
                it.second.erase(TransactionID);

                mu.lock();
                cout << "Unlock [" << TransactionID << ", " << it.first << "]\n";
                mu.unlock();

                pthread_cond_broadcast(&condition_var[it.first]);
            }
        }
        // storing the variables that the transaction has write lock on
        vector <string> tmp;
        for(auto it:writeLock){
            if(it.second==TransactionID) tmp.push_back(it.first);
        }
        
        // releasing the write lock
        for(auto it:tmp){
            writeLock.erase(it);
            
            mu.lock();
            cout << "Unlock [" << TransactionID << ", " << it << "]\n";
            mu.unlock();

            // multiple threads might be waiting for a readlock which can be granted to all
            pthread_cond_broadcast(&condition_var[it]);
        }

        // unlocking mutex
        pthread_mutex_unlock(&lock);
    }
};


// DataBase to Store the Variable Values
class DataBase: public LockMgr {
    map<string, int> variables;
    map<string, int> backup;

public:
    // Constructor to intitialize readLock, writeLock and variables
    // arg_variables: <variable_name, its_initial_value>
    DataBase (vector <pair<string, int>> arg_variables) : LockMgr(arg_variables) {
        for (auto it:arg_variables) {
            variables[it.first] = it.second;
            backup[it.first] = it.second;
        }

        printState();
    }

    // Get the current value of a variable
    int Read(string variableName, string transactionID) {
        if (variables.find(variableName) == variables.end())
            throw "VariableNotFoundError";

        acquireReadLock(transactionID, variableName);
        backup[variableName] = variables[variableName];

        return variables[variableName];
    }
    
    // Update the value of an Existing Variable
    void Write(string variableName, int newValue, string transactionID) {
        if (variables.find(variableName) == variables.end())
            throw "VariableNotFoundError";

        if (readLock[variableName].find(transactionID) != readLock[variableName].end())
            upgradeLock(transactionID, variableName);
        else 
            acquireWriteLock(transactionID, variableName);

        backup[variableName] = variables[variableName];
        
        variables[variableName] = newValue;
    }

    void Commit(string transactionID) {
        vector <string> tmp;
        for(auto it:readLock){
            if(it.second.find(transactionID) != it.second.end()) tmp.push_back(it.first);
        }

        tmp.clear();

        for(auto it:writeLock){
            if(it.second==transactionID) tmp.push_back(it.first);
        }

        releaseLock(transactionID);

        mu.lock();
        cout << "Commit [" << transactionID << "]\n";
        mu.unlock();
    }

    void Abort(string transactionID) {
        vector <string> tmp;
        for(auto it:readLock){
            if(it.second.find(transactionID) != it.second.end()) tmp.push_back(it.first);
        }

        tmp.clear();

        for(auto it:writeLock){
            if(it.second==transactionID) tmp.push_back(it.first);
        }

        for (auto it:tmp) {
            variables[it] = backup[it];
        }

        releaseLock(transactionID);

        mu.lock();
        cout << "Abort [" << transactionID << "]\n";
        mu.unlock();
    }

    void printState() {
        mu.lock();
        for (auto it:variables) {
            cout << it.first << " " << it.second << " ";
        }
        cout << "\n";
        mu.unlock();
    }

    void executeTransaction(Transaction currTransaction) {
        map <string, int> tmp;

        // Execute Transaction Operations Line By Line
        for (auto command:currTransaction.operations) {

            vector <string> splitedCommand;
            string word;

            for (int i = 0; command[i] != '\0'; i++) {
                if (command[i] == ' ') {
                    splitedCommand.push_back(word);
                    word = "";
                }
                else {
                    word += command[i];
                }
            }
            splitedCommand.push_back(word);

            // Execute read operation
            if (splitedCommand[0] == "R") {
                tmp[splitedCommand[1]] = Read(splitedCommand[1], currTransaction.ID);
            }
            // Execute write operation
            else if (splitedCommand[0] == "W") {
                Write(splitedCommand[1], tmp[splitedCommand[1]], currTransaction.ID);
            }
            // Execute commit operation
            else if (splitedCommand[0] == "C" && splitedCommand.size() == 1) {
                Commit(currTransaction.ID);
            }
            // Execcute abort operation
            else if (splitedCommand[0] == "A" && splitedCommand.size() == 1) {
                Abort(currTransaction.ID);
            }
            // Execute Mathematical operation
            else {
                int calc = 0;
                int add = 1;
                for (int i = 2; i < splitedCommand.size(); i += 2) {
                    add = (splitedCommand[i-1] == "+" || splitedCommand[i-1] == "=") - (splitedCommand[i-1] == "-");
                    
                    if (tmp.find(splitedCommand[i]) != tmp.end()) {
                        calc += add*(tmp[splitedCommand[i]]);
                    }
                    else {
                        calc += add*stoi(splitedCommand[i]);
                    }
                }

                tmp[splitedCommand[0]] = calc;
            }
        }
    }

};

pair<DataBase, vector<Transaction> > parse(string file_name) {
    vector<Transaction> rv;

    ifstream file_obj;
    file_obj.open(file_name);

    string line;
    
    // Reading Number of Transacrions
    getline(file_obj, line);

    // Number of Transactions
    int N = stoi(line);
    
    // Reading list of Arguments
    getline(file_obj, line);

    // Parsing list of Arguments to a list
    vector<pair<string, int>> listVariables;
    string currVariableName;
    for(int i = 0; line[i] != '\0'; i++) {
        // Resetting currVariableName
        currVariableName = "";

        // Reading Variable name
        while (line[i] != ' ') {
            currVariableName += line[i++];
        }

        int num = 0;

        // skipping spaces
        while (line[i] == ' ') i++;

        // Reading Variable Number
        while (line[i] >= '0' && line[i] <= '9') {
            num = num * 10 + (line[i++] - '0');
        }

        listVariables.push_back({currVariableName, num});
    }

    // adding Variables to DataBase
    DataBase db(listVariables);

    // Reading N Transactions
    for (int i = 0; i < N; i++) {
        Transaction T;

        // Reading First line of Transaction i.e. Transaction ID
        getline(file_obj, line);
        T.ID = line;

        while (getline(file_obj, line)) {
            T.operations.push_back(line);
            if (line == "C" || line == "A"){
                T.outcome=((line=="C")?1:0);
                break;
            }
        }

        rv.push_back(T);
    }

    file_obj.close();

    return {db, rv};
} 

DataBase *db;
map <string, bool> transactionComplete;
vector<Transaction> listTransactions;

// parameters are passed as a void pointer by convention
// in the thread function and it returns a void pointer too.
void* execute(void * arg) {
    // Converting the void pointer first into a pointer object
    // of Transaction class and then dereferencing it
    Transaction T = *((Transaction *)arg);

    // Transaction Begins
    transactionComplete[T.ID] = false;

    // Execute Transaction
    db->executeTransaction(T);

    // Transaction Ends
    transactionComplete[T.ID] = true;

    // Exiting thread
    pthread_exit(NULL);
}

// ctrl+c signal callback function
// prints the transaction ID's of all pending transactions
void signal_callback(int sigint) {
    cout << "Transactions in a DeadLock are : ";
    for (auto i:transactionComplete) {
        if (i.second == false) {
            cout << i.first << " ";
        }
    }
    cout << "\n";
}

int main(int argc, char *argv[]) {
    string filename = "input1.txt";

    if (argc == 2) {
        filename = argv[1];
    }

    auto x = parse(filename);

    db = &(x.first);
    listTransactions = x.second;

    // Asking Database to Execute All Transactions
    // Different Threads to be made here

    //No of transactions
    int N=listTransactions.size();
    //Declaring an array of threads to execute each transaction
    //on a separate thread
    pthread_t trd[N];

    for(int i=0; i<N; i++){
        // Execute on a new thread
        pthread_create(&(trd[i]),NULL,execute,&listTransactions[i]);
    }

    // For Bonus
    // Setting signal_callback function to run when user presses ctlr+c
    signal(SIGINT, signal_callback);

    void* status;
    //Joining each thread with the main function
    //so that the main doesn't exit before the created threads
    for(int i=0;i<N;i++){
        pthread_join(trd[i],&status);
    }

    // Printing the final value of all variables
    cout << "Result : "; db->printState();
    return 0;
}
