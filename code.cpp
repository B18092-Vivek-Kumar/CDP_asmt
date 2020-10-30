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

protected:
    pthread_mutex_t lock;
    map <string, pthread_cond_t> condition_var;
    map <string, set<string>> readLock;
    map <string, string> writeLock;
    //string Default = "\0";

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

    void releaseLock (string transactionID, string variableWithLock){

        //release the readLocks of this transaction
        for(auto &it:readLock){
            if(it.second.find(transactionID)!=it.second.end()){
                it.second.erase(transactionID);
            }
        }
        //release the writeLocks of this transaction
        if (writeLock.find(variableWithLock) != writeLock.end()) {
            if (writeLock[variableWithLock] == transactionID)
                writeLock.erase(variableWithLock);
        }
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

        cout << "Variables Initialized as : ";
        printState();
    }

    // Get the current value of a variable
    int Read(string variableName, string transactionID) {
        if (variables.find(variableName) == variables.end())
            throw "VariableNotFoundError";

        cout << transactionID <<" trying to Read " << variableName << "\n"; 

        acquireReadLock(transactionID, variableName);
        backup[variableName] = variables[variableName];

        cout << transactionID << " acquired ReadLock on " << variableName << "\n";
        
        return variables[variableName];
    }
    
    // Update the value of an Existing Variable
    void Write(string variableName, int newValue, string transactionID) {
        if (variables.find(variableName) == variables.end())
            throw "VariableNotFoundError";
        
        cout << transactionID <<" trying to OverWrite " << variableName << "\n"; 
        
        if (readLock[variableName].find(transactionID) != readLock[variableName].end())
            upgradeLock(transactionID, variableName);
        else 
            acquireWriteLock(transactionID, variableName);
        
        cout << transactionID << " acquired WriteLock on " << variableName << "\n";

        backup[variableName] = variables[variableName];
        
        variables[variableName] = newValue;
    }

    void Commit(string transactionID) {
        for (auto i:writeLock) {
            if (i.second == transactionID) {
                releaseLock(transactionID, i.first);
                cout << transactionID << " released Locks on " << i.first << "\n";
            }
        }

        for (auto i:readLock) {
            if (i.second.find(transactionID) != i.second.end()) {
                releaseLock(transactionID, i.first);
                cout << transactionID << " released  Locks on " << i.first << "\n";
            }
        }

        cout << transactionID << " Commited !!" << "\n";
    }

    void Abort(string transactionID) {
        for (auto i:writeLock) {
            if (i.second == transactionID) {
                variables[i.first] = backup[i.first];
                releaseLock(transactionID, i.first);
                cout << transactionID << " released Locks on " << i.first << "\n";
            }
        }

        for (auto i:readLock) {
            if (i.second.find(transactionID) != i.second.end()) {
                releaseLock(transactionID, i.first);
                cout << transactionID << " released  Locks on " << i.first << "\n";
            }
        }

        cout << transactionID << " Aborted !!" << "\n";
    }

    void printState() {
        for (auto it:variables) {
            cout << it.first << ":" << it.second << " , ";
        }
        cout << "\n";
    }

    // parameters are passed as a void pointer by convention
    // in the thread function and it returns a void pointer too.
    void executeTransaction(Transaction currTransaction) {
        // Converting the void pointer first into a pointer object
        // of Transaction class and then dereferencing it
        // Transaction currTransaction = *((Transaction*)arg);

        map <string, int> tmp;

        cout << currTransaction.ID << " Executing, DataBase State "; printState();

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

            if (splitedCommand[0] == "R") {
                tmp[splitedCommand[1]] = Read(splitedCommand[1], currTransaction.ID);
            }
            else if (splitedCommand[0] == "W") {
                Write(splitedCommand[1], tmp[splitedCommand[1]], currTransaction.ID);
            }
            else if (splitedCommand[0] == "C") {
                Commit(currTransaction.ID);
            }
            else if (splitedCommand[0] == "A") {
                Abort(currTransaction.ID);
            }
            else if (variables.find(splitedCommand[0]) == variables.end()) {
                throw "VariableNotFoundError";
            }
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

        printState();

        pthread_exit(NULL);
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

void* execute(void * arg) {
    // Converting the void pointer first into a pointer object
    // of Transaction class and then dereferencing it
    auto arg12 = *((pair<DataBase *, Transaction *>*)arg);

    DataBase* db = arg12.first;
    Transaction* T = arg12.second;

    cout << T->ID << " recieved by execute\n";

    db->executeTransaction(*T);
}

int main() {
    string filename = "input1.txt";
    auto x = parse(filename);

    DataBase db = x.first;
    vector<Transaction> listTransactions = x.second;

    // Asking Database to Execute All Transactions
    // Different Threads to be made here

    //No of transactions
    int N=listTransactions.size();
    //Declaring an array of threads to execute each transaction
    //on a separate thread
    pthread_t trd[N];

    vector <pair<DataBase *, Transaction *>> arg;

    for(int i=0; i<N; i++){
        arg.push_back(make_pair(&db, &listTransactions[i]));

        cout << "Sending " << arg[i].second->ID << " over to execute \n";
        
        // Execute on a new thread
        pthread_create(&(trd[i]),NULL,execute,&arg[i]);
    }


    void* status;
    //Joining each thread with the main function
    //so that the main doesn't exit before the created threads
    for(int i=0;i<N;i++){
        pthread_join(trd[i],&status);
    }

    cout << "\n\n\n\nResult : "; db.printState();
    return 0;
}
