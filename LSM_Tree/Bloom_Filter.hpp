//
//  Bloom_Filter.hpp
//  LSM_Tree
//
//  Created by Shiyu Huang on 3/4/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#ifndef Bloom_Filter_hpp
#define Bloom_Filter_hpp

#include <stdio.h>
#include <vector>


/*
 definition of Bloom filter for inserting integer
 */
class BloomFilter {
    
    unsigned int m_numHashes;
    std::vector<bool> m_bits;
    unsigned long int prime;
    int random1;
    int random2;
    /*
     Generate a prime bigger equal than size of the filter
     */
    bool check_prime(unsigned long int num){
        for(int i = 2; i < num/2; i++){
            if(num%i == 0){
                return false;
            }
        }
        return true;
    }
    unsigned long int generate_prime(){
        bool stop = false;
        unsigned long int current = m_bits.size();
        while(!stop){
            if(check_prime(current)){
                stop = true;
            }else{
                current++;
            }
        }
        return current;
    };
    /*
     A simple universal hash function
     https://en.wikipedia.org/wiki/Universal_hashing, hashing integer part, family of simple universal hash functions
     formula: h_a(x) = (ax mod p) mod m, where a is a random number, m is the codomain of the hash function,
     p is a prime with p >= m
     */
    unsigned long int hashFunction(int a, int x){
        return ((a*x)%prime)%m_bits.size();
    };
    
    
public:
    const int SEED = 123454;
    BloomFilter(unsigned long int numEntries, double falsePosRate);
    void add(int data);
    bool possiblyContains(int data);
    void reset();
    unsigned long int ithHash(int i, int x);

};

#endif /* Bloom_Filter_hpp */
