//
// Created by zhachen on 2019-05-13.
//

#ifndef BREEZE_CPP_BREEZE_H
#define BREEZE_CPP_BREEZE_H
#define LOG_WARN(fmt, ...)
#define LOG_ERROR(fmt, ...)

#if (defined(__clang__) && (__GNUC__ > 3)) || (!defined(__clang__) && (__GNUC__ > 6))

#include <any>

#endif

#include <boost/any.hpp>
#include <memory>
#include <cmath>
#include <string>
#include <vector>
#include <iostream>
#include <typeinfo>
#include <unordered_map>
#include "common/buffer.h"

enum BreezeType {
    kBreezeStringType = 0x3f,
    kBreezeDirectStringMinType = 0x00,
    kBreezeDirectStringMaxType = 0x3e,
    kBreezeInt32Type = 0x7f,
    kBreezeDirectInt32MinType = 0x40,
    kBreezeDirectInt32MaxType = 0x7e,
    kBreezeInt64Type = 0x98,
    kBreezeDirectInt64MinType = 0x80,
    kBreezeDirectInt64MaxType = 0x97,
    kBreezeNullType = 0x99,
    kBreezeTrueType = 0x9a,
    kBreezeFalseType = 0x9b,
    kBreezeByteType = 0x9c,
    kBreezeBytesType = 0x9d,
    kBreezeInt16Type = 0x9e,
    kBreezeFloat32Type = 0x9f,
    kBreezeFloat64Type = 0xa0,
    kBreezeMapType = 0xd9,
    kBreezeArrayType = 0xda,
    kBreezePackedMapType = 0xdb,
    kBreezePackedArrayType = 0xdc,
    kBreezeSchemaType = 0xdd,
    kBreezeMessageType = 0xde,
    kBreezeRefMessageType = 0xdf,
    kBreezeDirectRefMessageMaxType = 0xff,

    kBreezeInt32Zero = 0x50,
    kBreezeInt64Zero = 0x88,
    kBreezeDirectStringMaxLength = kBreezeDirectStringMaxType,
    kBreezeDirectInt32MinValue = kBreezeDirectInt32MinType - kBreezeInt32Zero,
    kBreezeDirectInt32MaxValue = kBreezeDirectInt32MaxType - kBreezeInt32Zero,
    kBreezeDirectInt64MinValue = kBreezeDirectInt64MinType - kBreezeInt64Zero,
    kBreezeDirectInt64MaxValue = kBreezeDirectInt64MaxType - kBreezeInt64Zero,
    kBreezeDirectRefMessageMaxValue = kBreezeDirectRefMessageMaxType - kBreezeRefMessageType,

    kBreezeMaxElemSize = 100000
};

class BreezeField {
public:
    BreezeField(int i, std::string n, std::string t);

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
        buf->write_byte(kBreezeMessageType);
        write_value(buf, name, false);
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
        buf->write_varint(index);
        if (auto err = write_value(buf, v, true)) {
            LOG_ERROR("write_value err:{}", err);
            return err;
        }
        return MOTAN_OK;
    }

    static int write_value(BytesBuffer *buf, const BreezeMessage &v, bool with_type);

    template<typename T>
    static int write_value(BytesBuffer *buf, const std::vector<T> &v, bool with_type) {
        if (can_pack_array(v)) {
            return MOTAN_OK;
        } else {
            if (with_type) {
                buf->write_byte(kBreezeArrayType);
            }
            buf->write_varint(v.size());
            int err{};
            for (const auto &x:v) {
                if ((err = write_value(buf, x, true))) {
                    return err;
                }
            }
            return MOTAN_OK;
        }
    }

    template<typename K, typename V>
    static int write_value(BytesBuffer *buf, const std::unordered_map<K, V> &v, bool with_type) {
        if (can_pack_map(v)) {
            return MOTAN_OK;
        } else {
            if (with_type) {
                buf->write_byte(kBreezeMapType);
            }
            buf->write_varint(v.size());
            int err{};
            for (const auto &x:v) {
                if ((err = write_value(buf, x.first, true))) {
                    return err;
                }
                if ((err = write_value(buf, x.second, true))) {
                    return err;
                }
            }
            return MOTAN_OK;
        }
    }

    //TODO 如何判断参数是否为map或vector?
    template<typename T>
    static bool can_pack_array(const std::vector<T> &v) {
        return false;
    }

    template<typename K, typename V>
    static bool can_pack_map(const std::unordered_map<K, V> &v) {
        return false;
    }

    static int write_value(BytesBuffer *buf, const std::nullptr_t &v, bool with_type);

    static int write_value(BytesBuffer *buf, const std::string &s, bool with_type);

    static int write_value(BytesBuffer *buf, const uint16_t &v, bool with_type);

    static int write_value(BytesBuffer *buf, const int16_t &v, bool with_type);

    static int write_value(BytesBuffer *buf, const uint32_t &v, bool with_type);

    static int write_value(BytesBuffer *buf, const int32_t &v, bool with_type);

    static int write_value(BytesBuffer *buf, const uint64_t &v, bool with_type);

    static int write_value(BytesBuffer *buf, const int64_t &v, bool with_type);

    static int write_value(BytesBuffer *buf, const float_t &v, bool with_type);

    static int write_value(BytesBuffer *buf, const double_t &v, bool with_type);

    static int write_value(BytesBuffer *buf, const bool &v, bool with_type);

    static int write_value(BytesBuffer *buf, const uint8_t &v, bool with_type);

    static int write_value(BytesBuffer *buf, const std::vector<uint8_t> &v, bool with_type);

    template<typename T>
    static int read_message_by_field(BytesBuffer *buf, T read_field) {
        uint32_t total = 0;
        int err{};
        if ((err = buf->read_uint32(total))) {
            LOG_ERROR("read_uint32 error:{}", err);
            return err;
        }
        if (total > 0) {
            auto pos = buf->read_pos_;
            auto endPos = pos + total;
            uint64_t index = 0;
            while (buf->read_pos_ < endPos) {
                if ((err = buf->read_varint(index))) {
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
    static int read_value(BytesBuffer *buf, std::vector<T> &v, bool with_type, uint8_t t, const std::string &msg_name) {
        uint8_t type;
        int err{};
        if (with_type && (err = buf->read_byte(type))) {
            LOG_ERROR("read_byte err:{}", err);
            return err;
        }
        return ReadArray(buf, v, type == kBreezePackedArrayType);
    }

    template<typename T>
    static int ReadArray(BytesBuffer *buf, std::vector<T> &v, bool isPacked) {
        uint64_t size = 0;
        int err{};
        if ((err = buf->read_varint(size))) {
            LOG_ERROR("read_varint error:{}", err);
            return err;
        }
        if (size <= 0) {
            return MOTAN_OK;
        }
        if (size > kBreezeMaxElemSize) {
            LOG_ERROR("collection elem size overflow. size:{}", size);
            return E_MOTAN_OVERFLOW;
        }
        uint8_t type{};
        if (isPacked) {
            if ((err = buf->read_byte(type))) {
                LOG_ERROR("read_byte err:{}", err);
                return err;
            }
        }
        for (int i = 0; i < size; i++) {
            T tv{};
            if ((err = read_value(buf, tv, !isPacked, type, ""))) {
                return err;
            }
            v.push_back(tv);
        }
        return MOTAN_OK;
    }

    template<typename K, typename V>
    static int read_value(BytesBuffer *buf, std::unordered_map<K, V> &v, bool with_type, uint8_t t, const std::string &msg_name) {
        uint8_t type;
        int err{};
        if (with_type && (err = buf->read_byte(type))) {
            LOG_ERROR("read_byte err:{}", err);
            return err;
        }
        return read_map(buf, v, type == kBreezePackedMapType);
    }

    template<typename K, typename V>
    static int read_map(BytesBuffer *buf, std::unordered_map<K, V> &v, bool isPacked) {
        uint64_t size = 0;
        int err = 0;
        if ((err = buf->read_varint(size))) {
            LOG_ERROR("read_varint error:{}", err);
            return err;
        }
        if (size <= 0) {
            return MOTAN_OK;
        }
        if (size > kBreezeMaxElemSize) {
            LOG_ERROR("collection elem size overflow. size:{}", size);
            return E_MOTAN_OVERFLOW;
        }
        uint8_t key_type{}, value_type{};
        if (isPacked) {
            if ((err = buf->read_byte(key_type))) {
                LOG_ERROR("read_byte err:{}", err);
                return err;
            }
            if ((err = buf->read_byte(value_type))) {
                LOG_ERROR("read_byte err:{}", err);
                return err;
            }
        }
        for (int i = 0; i < size; i++) {
            K key{};
            if ((err = read_value(buf, key, !isPacked, key_type, ""))) {
                return err;
            }
            V value{};
            if ((err = read_value(buf, value, !isPacked, value_type, ""))) {
                return err;
            }
            v[key] = value;
        }
        return MOTAN_OK;
    }

    static int read_value(BytesBuffer *buf, BreezeMessage &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_message(BytesBuffer *buf, BreezeMessage &v);

    static int read_value(BytesBuffer *buf, BreezeEnum &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_enum(BytesBuffer *buf, BreezeEnum &v);

    static int read_value(BytesBuffer *buf, std::string &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, std::nullptr_t &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, bool &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, uint8_t &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, std::vector<uint8_t> &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, int16_t &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, uint16_t &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, int32_t &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, uint32_t &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, int64_t &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, uint64_t &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, float_t &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_value(BytesBuffer *buf, double_t &res, bool with_type, uint8_t t, const std::string &msg_name);

    static int read_string(BytesBuffer *buf, std::string &v);

    static int read_bytes(BytesBuffer *buf, std::vector<uint8_t> &v);

    static bool check_type(BytesBuffer *buf, uint8_t expect);

    static int read_value(BytesBuffer *buf, boost::any &v, bool with_type, uint8_t t, const std::string &msg_name);

    static int write_value(BytesBuffer *buf, const boost::any &v, bool with_type);

#if (defined(__clang__) && (__GNUC__ > 3)) || (!defined(__clang__) && (__GNUC__ > 6))

    static int read_value(BytesBuffer *buf, std::any &v, bool with_type, uint8_t t, const std::string &msg_name);

    static int write_value(BytesBuffer *buf, const std::any &v, bool with_type);

#endif
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

    boost::any get_field_by_index(const int32_t &index);

    boost::any get_field_by_name(const std::string &name);

    void put_field(const int32_t &index, const boost::any &field);

private:
    std::shared_ptr<BreezeSchema> schema_{};
    std::unordered_map<int32_t, boost::any> fields_{};
};

#endif //MOTAN_CPP_BREEZE_H
