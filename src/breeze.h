//
// Created by zhachen on 2019-05-23.
//

#ifndef BREEZE_CPP_BREEZE_H
#define BREEZE_CPP_BREEZE_H
#define LOG_WARN(fmt, ...)
#define LOG_ERROR(fmt, ...)

#include <variant>
#include <any>
#include <cmath>
#include <string>
#include <vector>
#include <iostream>
#include <memory>
#include <typeinfo>
#include <unordered_map>
#include "common/buffer.h"

//定义anyMap的key类型，因为any对象无法hash不能做key，variant又无法接收breeze类型，所以需要两种类型
typedef std::variant<bool, uint8_t, int16_t, int32_t, int64_t, float_t, double_t, std::string> BREEZE_VARIANT;

enum BreezeType {
    kBreezeNull = 0,
    kBreezeTrue = 1,
    kBreezeFalse = 2,
    kBreezeString = 3,
    kBreezeByte = 4,
    kBreezeBytes = 5,
    kBreezeInt16 = 6,
    kBreezeInt32 = 7,
    kBreezeInt64 = 8,
    kBreezeFloat32 = 9,
    kBreezeFloat64 = 10,

    kBreezeMap = 20,
    kBreezeArray = 21,
    kBreezeMessage = 22,
    kBreezeSchema = 23,
};

class BreezeField {
public:
    int index_{};
    std::string name_{};
    std::string type_{};
};

class BreezeSchema {
public:
    std::string name_{};
    std::string alias_{};

    void put_field(const std::shared_ptr<BreezeField> &field);

    std::shared_ptr<BreezeField> get_field_by_index(const int32_t &index);

    std::shared_ptr<BreezeField> get_field_by_name(const std::string &name);

private:
    std::unordered_map<int, std::shared_ptr<BreezeField >> index_fields_{};
    std::unordered_map<std::string, std::shared_ptr<BreezeField >> name_fields_{};
};

class BreezeMessage {
public:
    virtual int write_to(BytesBuffer *) const = 0;

    virtual int read_from(BytesBuffer *) = 0;

    virtual std::string get_name() const = 0;

    virtual void set_name(const std::string &name) = 0;

    virtual std::string get_alias() = 0;

    virtual std::shared_ptr<BreezeSchema> get_schema() = 0;
};

class BreezeEnum : public BreezeMessage {
public:
    virtual int read_enum(BytesBuffer *) = 0;
};

class breeze {
public:
    template<typename T>
    static int write_message(BytesBuffer *buf, const std::string &name, T func) {
        buf->write_byte(kBreezeMessage);
        write_value(buf, name);
        auto pos = buf->write_pos_;
        buf->write_pos_ += 4;
        func();
        auto npos = buf->write_pos_;
        buf->write_pos_ = pos;
        buf->write_uint32(npos - pos - 4);
        buf->write_pos_ = npos;
        return MOTAN_OK;
    }

    template<typename T>
    static int write_message_field(BytesBuffer *buf, const int &index, const T &v) {
        buf->write_zigzag32(index);
        if (auto err = write_value(buf, v)) {
            LOG_ERROR("write_value err:{}", err);
            return err;
        }
        return MOTAN_OK;
    }

    static int write_value(BytesBuffer *buf, const BreezeMessage &v);

    template<typename T>
    static int write_value(BytesBuffer *buf, const std::vector<T> &v) {
        buf->write_byte(kBreezeArray);
        auto pos = buf->write_pos_;
        buf->write_pos_ += 4;
        int err;
        for (const auto &x:v) {
            if ((err = write_value(buf, x))) {
                return err;
            }
        }
        auto npos = buf->write_pos_;
        buf->write_pos_ = pos;
        buf->write_uint32(npos - pos - 4);
        buf->write_pos_ = npos;
        return MOTAN_OK;
    }

    template<typename K, typename V>
    static int write_value(BytesBuffer *buf, const std::unordered_map<K, V> &v) {
        buf->write_byte(kBreezeMap);
        auto pos = buf->write_pos_;
        buf->write_pos_ += 4;
        int err;
        for (const auto &x:v) {
            if ((err = write_value(buf, x.first))) {
                return err;
            }
            if ((err = write_value(buf, x.second))) {
                return err;
            }
        }
        auto npos = buf->write_pos_;
        buf->write_pos_ = pos;
        buf->write_uint32(npos - pos - 4);
        buf->write_pos_ = npos;
        return MOTAN_OK;
    }

    static int write_value(BytesBuffer *buf, const std::any &v);

    static int write_value(BytesBuffer *buf, const BREEZE_VARIANT &v);

    static int write_value(BytesBuffer *buf, const std::nullptr_t &v);

    static int write_value(BytesBuffer *buf, const std::string &s);

    static int write_value(BytesBuffer *buf, const uint16_t &v);

    static int write_value(BytesBuffer *buf, const int16_t &v);

    static int write_value(BytesBuffer *buf, const uint32_t &v);

    static int write_value(BytesBuffer *buf, const int32_t &v);

    static int write_value(BytesBuffer *buf, const uint64_t &v);

    static int write_value(BytesBuffer *buf, const int64_t &v);

    static int write_value(BytesBuffer *buf, const float_t &v);

    static int write_value(BytesBuffer *buf, const double_t &v);

    static int write_value(BytesBuffer *buf, const bool &v);

    static int write_value(BytesBuffer *buf, const uint8_t &v);

    static int write_value(BytesBuffer *buf, const std::vector<uint8_t> &v);

    template<typename T>
    static int read_message_by_field(BytesBuffer *buf, T read_field) {
        uint32_t total = 0;
        int err;
        if ((err = buf->read_uint32(total))) {
            LOG_ERROR("read_uint32 error:{}", err);
            return err;
        }
        if (total > 0) {
            auto pos = buf->read_pos_;
            auto endPos = pos + total;
            uint64_t index = 0;
            while (buf->read_pos_ < endPos) {
                if ((err = buf->read_zigzag32(index))) {
                    LOG_ERROR("read_zigzag32 error:{}", err);
                    return err;
                }
                if ((err = read_field(buf, index))) {
                    LOG_ERROR("read_field error:{}", err);
                    return err;
                }
            }
            if (buf->read_pos_ != endPos) {
                LOG_ERROR("read size error");
                return E_MOTAN_WRONG_SIZE;
            }
        }
        return MOTAN_OK;
    }

    template<typename T>
    static int read_value(BytesBuffer *buf, std::vector<T> &v) {
        if (!check_type(buf, kBreezeArray)) {
            LOG_ERROR("mismatch type");
            return E_MOTAN_MISMATCH_TYPE;
        }
        return ReadArray(buf, v);
    }

    template<typename T>
    static int ReadArray(BytesBuffer *buf, std::vector<T> &v) {
        uint32_t size = 0;
        int err;
        if ((err = buf->read_uint32(size))) {
            LOG_ERROR("read_uint32 error:{}", err);
            return err;
        }
        if (size <= 0) {
            return MOTAN_OK;
        }
        auto pos = buf->read_pos_;
        auto endPos = pos + size;
        while (buf->read_pos_ < endPos) {
            T tv{};
            if ((err = read_value(buf, tv))) {
                return err;
            }
            v.push_back(tv);
        }
        if (buf->read_pos_ != endPos) {
            LOG_ERROR("read size error");
            return E_MOTAN_WRONG_SIZE;
        }
        return MOTAN_OK;
    }

    template<typename K, typename V>
    static int read_value(BytesBuffer *buf, std::unordered_map<K, V> &v) {
        if (!check_type(buf, kBreezeMap)) {
            LOG_ERROR("mismatch type");
            return E_MOTAN_MISMATCH_TYPE;
        }
        return read_map(buf, v);
    }

    template<typename K, typename V>
    static int read_map(BytesBuffer *buf, std::unordered_map<K, V> &v) {
        uint32_t size = 0;
        int err = 0;
        if ((err = buf->read_uint32(size))) {
            LOG_ERROR("read_uint32 error:{}", err);
            return err;
        }
        if (size <= 0) {
            return MOTAN_OK;
        }
        auto endPos = buf->read_pos_ + size;
        while (buf->read_pos_ < endPos) {
            K key{};
            if ((err = read_value(buf, key))) {
                return err;
            }
            V value{};
            if ((err = read_value(buf, value))) {
                return err;
            }
            v[key] = value;
        }
        if (buf->read_pos_ != endPos) {
            LOG_ERROR("read size error");
            return E_MOTAN_WRONG_SIZE;
        }
        return MOTAN_OK;
    }

    static int read_value(BytesBuffer *buf, BREEZE_VARIANT &v);

    static int read_value(BytesBuffer *buf, std::any &v);

    static int read_value(BytesBuffer *buf, BreezeMessage &v);

    static int read_value(BytesBuffer *buf, BreezeEnum &v);

    static int read_message(BytesBuffer *buf, BreezeMessage &v);

    static int read_enum(BytesBuffer *buf, BreezeEnum &v);

    static int read_value(BytesBuffer *buf, std::string &v);

    static int read_value(BytesBuffer *buf, std::nullptr_t &i);

    static int read_value(BytesBuffer *buf, bool &i);

    static int read_value(BytesBuffer *buf, uint8_t &i);

    static int read_value(BytesBuffer *buf, std::vector<uint8_t> &i);

    static int read_value(BytesBuffer *buf, int16_t &i);

    static int read_value(BytesBuffer *buf, uint16_t &i);

    static int read_value(BytesBuffer *buf, int32_t &i);

    static int read_value(BytesBuffer *buf, uint32_t &i);

    static int read_value(BytesBuffer *buf, int64_t &i);

    static int read_value(BytesBuffer *buf, uint64_t &i);

    static int read_value(BytesBuffer *buf, float_t &i);

    static int read_value(BytesBuffer *buf, double_t &i);

    static int read_string(BytesBuffer *buf, std::string &v);

    static int read_bytes(BytesBuffer *buf, std::vector<uint8_t> &v);

    static bool check_type(BytesBuffer *buf, uint8_t expect);
};

class GenericMessage : public BreezeMessage {
public:
    std::string name_{};
    std::string alias_{};

    GenericMessage() = default;

    GenericMessage(std::string name, std::string alias);

    int write_to(BytesBuffer *) const override;

    int read_from(BytesBuffer *) override;

    std::string get_name() const override;

    void set_name(const std::string &name) override;

    std::string get_alias() override;

    std::shared_ptr<BreezeSchema> get_schema() override;

    std::any get_field_by_index(const int32_t &index);

    std::any get_field_by_name(const std::string &name);

    void put_field(const int32_t &index, const std::any &field);

private:
    std::shared_ptr<BreezeSchema> schema_{};
    std::unordered_map<int32_t, std::any> fields_{};
};

#endif //BREEZE_CPP_BREEZE_H