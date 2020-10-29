#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <fstream>
#include <vector>
#include <map>
#include <set>

using namespace std;

class Transaction {
public:
    // ID_Format = Tx012
    string ID;

    // operations : <operation_type, variable_Name>
    // operation_type:
    //      "R"
    //      "W"
    //      "+ some_number", "- some_number"
    //      "+ some_variable_name", "- some_variable_name"
    vector <string> operations;
    
    // final outcome i.e. commited or aborted
    bool outcome;
};

class LockMgr {
    // Lock : <variableName, TransactionID>
    // Default Value for TransactionID = '\0' = 0
    // Default Value is assigned if variable in Question is unlocked
    // condition_var : <variableName,conditionVariable>

    pthread_mutex_t lock;
    map <string, pthread_cond_t> condition_var;
    map <string, set<string>> readLock;
    map <string, string> writeLock;
    //string Default = "\0";

protected:
    // Constructor to intitialize readLock, writeLock, lock and conditon variables
    // arg_variables: <variable_name, its_initial_value>
    LockMgr (vector <pair<string, int> > arg_variables) {
        lock=PTHREAD_MUTEX_INITIALIZER;

        for (auto it:arg_variables) {
            // readLock[it.first];
            // writeLock[it.first] = Default;
            condition_var[it.first]=PTHREAD_COND_INITIALIZER;
        }

        cout << "Locks Initialized" << "\n";
    }


    void acquireReadLock (string TransactionID, string variableName){
        //condition varibles are always used with a lock
        pthread_mutex_lock(&lock);

        //check if there is already a writeLock on that variable by some other thread
        while(writeLock.find(variableName)!=writeLock.end())
            pthread_cond_wait(&condition_var[variableName], &lock);
        
        //insert the transaction id into the set for that variable
        readLock[variableName].insert(TransactionID);
        pthread_mutex_unlock(&lock);
    }

    void acquireWriteLock (string TransactionID, string variableName){
        pthread_mutex_lock(&lock);

        //check if there is already a writeLock or a readLock on that variable by some other thread
        while(writeLock.find(variableName)!=writeLock.end() || readLock.find(variableName)!=readLock.end())
            pthread_cond_wait(&condition_var[variableName], &lock);
        
        writeLock[variableName]=TransactionID;
        pthread_mutex_unlock(&lock);
    }

    void upgradeLock (string TransactionID, string variableName){
        pthread_mutex_lock(&lock);

        //if some other thread is reading or writing this variable, put this thread on sleep
        while (readLock[variableName].size()>1 || writeLock.find(variableName)!=writeLock.end())              
            pthread_cond_wait(&condition_var[variableName], &lock);

        readLock[variableName].clear();
        writeLock[variableName]=TransactionID;
        pthread_mutex_unlock(&lock);
    }

    //void downgradeLock (string TransactionID, string variableName) {}

    void releaseLock (string TransactionID, string variable_With_WriteLock){

        //release the readLocks of this transaction
        for(auto &it:readLock){
            if(it.second.find(TransactionID)!=it.second.end()){
                it.second.erase(TransactionID);
            }
        }
        //release the writeLocks of this transaction
        writeLock.erase(variable_With_WriteLock);
    }
};


// DataBase to Store the Variable Values
class DataBase: public LockMgr {
    map<string, int> variables;

public:
    // Constructor to intitialize readLock, writeLock and variables
    // arg_variables: <variable_name, its_initial_value>
    DataBase (vector <pair<string, int>> arg_variables) : LockMgr(arg_variables) {
        for (auto it:arg_variables) {
            variables[it.first] = it.second;
        }

        cout << "Variables Initialized" << "\n";
    }

    // Get the current value of a variable
    int Read(string variableName, string transactionID) {
        if (variables.find(variableName) == variables.end())
            throw "VariableNotFoundError";
        
        return variables[variableName];
    }
    
    // Update the value of an Existing Variable
    void Write(string variableName, int newValue, string transactionID) {
        if (variables.find(variableName) == variables.end())
            throw "VariableNotFoundError";
        
        variables[variableName] = newValue;
    }

    // parameters are passed as a void pointer by convention
    // in the thread function and it returns a void pointer too.
    void* executeTransaction(void* arg) {
        // Converting the void pointer first into a pointer object
        // of Transaction class and then dereferencing it
        Transaction currTransaction = *((Transaction*)arg);

        map <string, int> tmp;
        
        // Execute Transaction Operations Line By Line
        for (auto command:currTransaction.operations) {

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

int main() {
    string filename = "input1.txt";
    auto x = parse(filename);

    auto db = x.first;
    auto listTransactions = x.second;

    // Asking Database to Execute All Transactions
    // Different Threads to be made here

    //No of transactions
    int N=listTransaction.size();
    //Declaring an array of threads to execute each transaction
    //on a separate thread
    pthread_t trd[N];


    for(int i=0;i<N;i++){
        //Execute on a new thread
        //
        pthread_create(&(trd[i]),NULL,db.executeTransaction,&listTransactions[i]);
    }


    void* status;
    //Joining each thread with the main function
    //so that the main doesn't exit before the created threads
    for(int i=0;i<N;i++){
        pthread_join(trd[i],&status);
    }
    return 0;
}
