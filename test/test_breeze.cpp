//
// Created by zhachen on 2019-05-23.
//

#include <memory>
#include <limits>
#include <gtest/gtest.h>
#include "breeze.h"
#include "common/buffer.h"
#include "test_msg.breeze.h"

#define DEFAULT_BUFFER_SIZE 200

template<typename T, typename V>
void test_data(const T &data, V ret) {
    std::shared_ptr<BytesBuffer> buf(new_bytes_buffer(DEFAULT_BUFFER_SIZE));
    auto err = breeze::write_value(buf.get(), data);
    ASSERT_EQ(err, MOTAN_OK);
    err = breeze::read_value(buf.get(), ret);
    ASSERT_EQ(err, MOTAN_OK);
    ASSERT_EQ(data, ret);
}

template<typename T, typename V>
void test_data_list(const T &data, V ret) {
    for (const auto &i : data) {
        test_data(i, ret);
    }
}

TEST(BasicType, Null) {
    nullptr_t data[1]{nullptr};
    nullptr_t ret{};
    test_data_list(data, ret);
}

TEST(BasicType, Bool) {
    bool data[2]{true, false};
    bool ret{};
    test_data_list(data, ret);
}

TEST(BasicType, String) {
    std::string data[2] = {"", "uwoerj8093lsd#!@#$%^^&&*()lkd"};
    std::string ret{};
    test_data_list(data, ret);
}

TEST(BasicType, Byte) {
    uint8_t data[4]{0, (1u << 7u) - 1, (1u << 8u) - 1, 36};
    uint8_t ret{};
    test_data_list(data, ret);
}

TEST(BasicType, Bytes) {
    std::vector<uint8_t> data[2]{std::vector<uint8_t>{}, std::vector<uint8_t>{'c', '&', '\n'}};
    std::vector<uint8_t> ret{};
    test_data_list(data, ret);
}

TEST(BasicType, Int16) {
    int16_t data[4]{0, 234, std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max()};
    int16_t ret{};
    test_data_list(data, ret);
}

TEST(BasicType, Int32) {
    int32_t data[4]{0, 12345, std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()};
    int32_t ret{};
    test_data_list(data, ret);
}

TEST(BasicType, Int64) {
    int64_t data[4]{0, 123, std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()};
    int64_t ret{};
    test_data_list(data, ret);
}

TEST(BasicType, Float) {
    float_t data[4]{0, 1.23, std::numeric_limits<float_t>::min(), std::numeric_limits<float_t>::max()};
    float_t ret{};
    test_data_list(data, ret);
}

TEST(BasicType, Double) {
    double_t data[4]{0, 1.23, std::numeric_limits<double_t>::min(), std::numeric_limits<double_t>::max()};
    double_t ret{};
    test_data_list(data, ret);
}

TEST(BasicType, Array) {
    std::vector<int32_t> data_int = {0, 1, 2};
    std::vector<int32_t> ret_int{};
    test_data(data_int, ret_int);

    std::vector<double_t> data_double = {1.23, 2.34, 3.45};
    std::vector<double_t> ret_double{};
    test_data(data_double, ret_double);

    std::vector<std::string> data_string = {"i", "love", "u"};
    std::vector<std::string> ret_string{};
    test_data(data_string, ret_string);
}

TEST(BasicType, Map) {
    std::unordered_map<int32_t, std::string> data = {{0, "0"},
                                                     {1, "1"},
                                                     {2, "2"}};
    std::unordered_map<int32_t, std::string> ret{};
    test_data(data, ret);
}

TEST(ComplexType, NestedMap) {
    std::unordered_map<int32_t, std::vector<float_t >> data_map = {{1, {1.1, 2.2}},
                                                                   {2, {2.2, 3.3}},
                                                                   {3, {3.3, 4.4}}};
    std::unordered_map<int32_t, std::vector<float_t >> ret_map{};
    test_data(data_map, ret_map);
}

TEST(ComplexType, NestedList) {
    std::vector<std::unordered_map<int32_t, std::string >> data_list = {{{1, "1"}, {2, "2"}, {3, "3"}},
                                                                        {{4, "4"}, {5, "5"}, {6, "6"}},
                                                                        {{7, "7"}, {8, "8"}, {9, "9"}}};
    std::vector<std::unordered_map<int32_t, std::string >> ret_list{};
    test_data(data_list, ret_list);
}

TEST(MessageType, BasiceMessage) {
    TestSubMsg msg{};
    msg.s = "hello";
    msg.i32 = 123;
    msg.i64 = 23456;
    msg.f = 1.23;
    msg.d = 123.456;
    msg.byte = 'b';
    msg.bytes = {'a', 'b', 'c'};
    msg.map1["love"] = {'u'};
    msg.map2[111] = {999};
    TestSubMsg ret{};
    std::shared_ptr<BytesBuffer> buf(new_bytes_buffer(DEFAULT_BUFFER_SIZE));
    auto err = breeze::write_value(buf.get(), msg);
    ASSERT_EQ(err, MOTAN_OK);
    err = breeze::read_value(buf.get(), ret);
    ASSERT_EQ(err, MOTAN_OK);
    ASSERT_EQ(ret.s, msg.s);
    ASSERT_EQ(ret.i32, msg.i32);
    ASSERT_EQ(ret.i64, msg.i64);
    ASSERT_EQ(ret.f, msg.f);
    ASSERT_EQ(ret.d, msg.d);
    ASSERT_EQ(ret.byte, msg.byte);
    ASSERT_EQ(ret.bytes, msg.bytes);
    ASSERT_EQ(ret.map1, msg.map1);
    ASSERT_EQ(std::any_cast<int32_t>(ret.map2[111][0]), std::any_cast<int32_t>(msg.map2[111][0]));
}

TEST(MessageType, NestedMessage) {
    TestSubMsg sub_arg{};
    sub_arg.map2[111] = {999};
    TestMsg arg{};
    arg.i = 10;
    arg.s = "zha";
    arg.a = std::vector<TestSubMsg>{sub_arg};
    std::shared_ptr<BytesBuffer> buf(new_bytes_buffer(DEFAULT_BUFFER_SIZE));
    auto err = breeze::write_value(buf.get(), arg);
    ASSERT_EQ(err, MOTAN_OK);
    TestMsg ret{};
    err = breeze::read_value(buf.get(), ret);
    ASSERT_EQ(err, MOTAN_OK);
    ASSERT_EQ(ret.s, arg.s);
    ASSERT_EQ(ret.i, arg.i);
    ASSERT_EQ(std::any_cast<int32_t>(ret.a[0].map2[111][0]), std::any_cast<int32_t>(arg.a[0].map2[111][0]));
}

TEST(MessageType, GenericMessage) {
    // TestSubMsg => GenericMessage
    TestSubMsg msg{};
    msg.s = "hello";
    msg.i32 = 123;
    msg.f = 1.23;
    std::shared_ptr<BytesBuffer> buf(new_bytes_buffer(DEFAULT_BUFFER_SIZE));
    auto err = breeze::write_value(buf.get(), msg);
    ASSERT_EQ(err, MOTAN_OK);
    GenericMessage ret_msg{};
    err = breeze::read_value(buf.get(), ret_msg);
    ASSERT_EQ(err, MOTAN_OK);
    ASSERT_EQ(msg.get_name(), ret_msg.get_name());
    ASSERT_EQ(msg.s, std::any_cast<std::string>(ret_msg.get_field_by_index(1)));
    ASSERT_EQ(msg.i32, std::any_cast<int32_t>(ret_msg.get_field_by_index(2)));
    ASSERT_EQ(msg.f, std::any_cast<float_t>(ret_msg.get_field_by_index(4)));

    // GenericMessage => GenericMessage
    GenericMessage data{};
    data.alias_ = data.name_ = "TestGeneric";
    data.put_field(1, "111");
    data.put_field(2, "222");
    data.put_field(3, "333");
    buf.reset(new_bytes_buffer(DEFAULT_BUFFER_SIZE));
    err = breeze::write_value(buf.get(), data);
    ASSERT_EQ(err, MOTAN_OK);
    GenericMessage ret{};
    err = breeze::read_value(buf.get(), ret);
    ASSERT_EQ(err, MOTAN_OK);
    ASSERT_EQ(data.get_name(), ret.get_name());
    ASSERT_EQ(std::any_cast<const char *>(data.get_field_by_index(1)),
              std::any_cast<std::string>(ret.get_field_by_index(1)));
}
