//
// Created by zhachen on 2019-05-13.
//

#ifndef MOTAN_CPP_TEST_MSG_BREEZE_H
#define MOTAN_CPP_TEST_MSG_BREEZE_H

#include "breeze.h"

class TestSubMsg : public BreezeMessage {
public:
    std::string s{};
    int32_t i32{};
    int64_t i64{};
    float_t f{};
    double_t d{};
    uint8_t byte{};
    std::vector<uint8_t> bytes{};
    std::unordered_map<std::string, std::vector<uint8_t >> map1{};
    std::unordered_map<int32_t, std::vector<std::any>> map2{};
    std::vector<int32_t> list{};
    bool b{};

    int write_to(BytesBuffer *buf) const override;

    int read_from(BytesBuffer *buf) override;

    std::string get_name() const override;

    std::string get_alias() override;

    std::shared_ptr<BreezeSchema> get_schema() override;

    void set_name(const std::string &name) override;
};

class TestMsg : public BreezeMessage {
public:
    std::string s{};
    int32_t i{};
    std::vector<TestSubMsg> a{};
    std::unordered_map<std::string, TestSubMsg> m{};

    int write_to(BytesBuffer *) const override;

    int read_from(BytesBuffer *) override;

    std::string get_name() const override;

    std::string get_alias() override;

    std::shared_ptr<BreezeSchema> get_schema() override;

    void set_name(const std::string &name) override;
};

#endif //MOTAN_CPP_TEST_MSG_BREEZE_H
