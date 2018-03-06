////
////  old_test.cpp
////  LSM_Tree
////
////  Created by Shiyu Huang on 3/5/18.
////  Copyright © 2018 Shiyu Huang. All rights reserved.
////
//
//#include "old_test.hpp"
//
//
//void merge_test(){
//    Buffer my_buffer;
//    my_buffer.put(4,8);
//    my_buffer.put(2,4);
//    my_buffer.put(1,90);
//    Layer my_layer;
//    my_buffer.sort();
//    my_layer.add_run_from_buffer(my_buffer);
//    my_buffer.put(4,5);
//    my_buffer.put(5,8);
//    my_buffer.put(20,9);
//    my_buffer.sort();
//    my_layer.add_run_from_buffer(my_buffer);
//    int size;
//    KVpair* run = my_layer.merge(size);
//    std::cout<<"f";
//}
//
//
//void buffer_test(){
//    Buffer *my_buffer = new Buffer();
//    my_buffer->put(1,1);
//    my_buffer->put(2,2);
//    my_buffer->put(3,3);
//    my_buffer->put(2,4);
//    my_buffer->del(3);
//
//    int query = NULL;
//    std::vector<KVpair> range;
//    if(my_buffer->get(1, query)){
//        std::cout << "The value is " <<query<<std::endl;
//    }else{
//        std::cout << "Point lookup not found "<<std::endl;
//    }
//    if(my_buffer->range(2,4,range)){
//        std::cout << "The kvpair in the range are " <<std::endl;
//        for(int i = 0; i < range.size(); i++){
//            std::cout << (range.at(i)).key <<" "<<(range.at(i)).value<<std::endl;
//        }
//    }else{
//        std::cout << "range lookup not found "<<std::endl;
//    }
//}
//
//void buffer_test2(){
//    Buffer my_buffer;
//    my_buffer.put(1,1);
//    my_buffer.put(2,2);
//    my_buffer.put(3,3);
//    my_buffer.put(2,4);
//    my_buffer.del(3);
//    int query = NULL;
//    std::vector<KVpair> range;
//    if(my_buffer.get(1, query)){
//        std::cout << "The value is " <<query<<std::endl;
//    }else{
//        std::cout << "Point lookup not found "<<std::endl;
//    }
//    if(my_buffer.range(2,4,range)){
//        std::cout << "The kvpair in the range are " <<std::endl;
//        for(int i = 0; i < range.size(); i++){
//            std::cout << (range.at(i)).key <<" "<<(range.at(i)).value<<std::endl;
//        }
//    }else{
//        std::cout << "range lookup not found "<<std::endl;
//    }
//}

