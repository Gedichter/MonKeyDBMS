//
//  Bloom_Filter.cpp
//  LSM_Tree
//
//  Created by Shiyu Huang on 3/4/18.
//  Copyright © 2018 Shiyu Huang. All rights reserved.
//

#include "Bloom_Filter.hpp"
#include <math.h>

/*
 false positive error rate:p
 size of the filter: m
 the number of hash functions: k
 number of entries inserted: n
 
 @param size the filter size in bits:m
 numHashes number of hash functions:k
 
 formula: if given p and n,
 m = -(n*ln(p))/ln^2(2), k = (m/n)*ln(2)
 TODO: can write a bloom filter parameter struct to compute parameters
 */
BloomFilter::BloomFilter(unsigned long int numEntries, double falsePosRate){
    unsigned long int numBits = (-1)*(numEntries*log(falsePosRate))/(log(2)*log(2));
    m_numHashes = (int)(numBits/numEntries)*log(2) + 0.5; //type cast always truncates
    m_bits = std::vector<bool>(numBits);
    prime = generate_prime();
    srand(SEED);
    random1 = 1+rand()%10000;
    random2 = 1+rand()%10000;    
};

unsigned long int BloomFilter::ithHash(int i, int x){
    return (hashFunction(random1, x) + i*hashFunction(random2, x))%m_bits.size();
};


void BloomFilter::add(int data){
    for(int i = 0; i < m_numHashes; i++){
        unsigned long int pos = ithHash(i, data);
        m_bits.at(pos) = true;
    }
};

bool BloomFilter::possiblyContains(int data){
    for(int i = 0; i < m_numHashes; i++){
        unsigned long int pos = ithHash(i, data);
        if(!m_bits.at(pos)){
            return false;
        }
    }
    return true;
};

void BloomFilter::reset(){
    for(int i = 0; i < m_bits.size(); i++){
        m_bits.at(i) = false;
    }
}
