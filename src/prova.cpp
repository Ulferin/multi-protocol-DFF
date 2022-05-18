#include <iostream>

class A_base {

protected:
    int info = 42;

public:
    void printAbstractName() {
        printf("Hello I'm A_base!\n");
    }
    virtual void printRealName() = 0;
};

class A_H : virtual public A_base {

protected:
    int infoB = 45;

public:
    void printAbstractName() {
        printf("Hello, I'm A_H!\n");
        A_base::printAbstractName();
    } 
};

class TCP_base : virtual public A_base {

public:
    void printInfo() {
        printf("Info is: %d\n", info);
    }

    void printRealName() {
        printf("My abstract ancestor is A_base! But I'm TCP_base!\n");
    }

};

class TCP_H : public TCP_base, public A_H {

public:
    void printRealName() {
        printf("Hello, my name is TCP_H!\n");
        TCP_base::printRealName();
    }

    void printInfo() {
        printf("InfoB is: %d\nInfo is: %d\n", infoB, info);
    }

};

int main(int argc, char** argv) {

    TCP_base c;
    c.printRealName();
    c.printInfo();
    c.printAbstractName();

    TCP_H d;
    d.printRealName();
    d.printInfo();
    d.printAbstractName();

    return 0;
}