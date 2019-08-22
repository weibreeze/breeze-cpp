//
// Created by zhachen on 2019-05-13.
//
#include "breeze.h"

BreezeField::BreezeField(int i, std::string n, std::string t) : index_(i), name_(n), type_(t) {}

void BreezeSchema::put_field(const std::shared_ptr<BreezeField> &field) {
    index_fields_[field->index_] = field;
    name_fields_[field->name_] = field;
}

std::shared_ptr<BreezeField> BreezeSchema::get_field_by_index(const int32_t &index) {
    auto iter = index_fields_.find(index);
    if (iter != index_fields_.end()) {
        return index_fields_[index];
    }
    return std::shared_ptr<BreezeField>(nullptr);
}

std::shared_ptr<BreezeField> BreezeSchema::get_field_by_name(const std::string &name) {
    auto iter = name_fields_.find(name);
    if (iter != name_fields_.end()) {
        return name_fields_[name];
    }
    return std::shared_ptr<BreezeField>(nullptr);
}

int breeze::write_value(BytesBuffer *buf, const BreezeMessage &v, bool with_type) {
    return v.write_to(buf);
}

// any只能是基础类型，不能嵌套
int breeze::write_value(BytesBuffer *buf, const boost::any &v, bool with_type) {
    if (v.type() == typeid(std::nullptr_t)) {
        write_value(buf, nullptr, true);
    } else if (v.type() == typeid(bool)) {
        write_value(buf, boost::any_cast<bool>(v), true);
    } else if (v.type() == typeid(std::string)) {
        write_value(buf, boost::any_cast<std::string>(v), true);
    } else if (v.type() == typeid(const char *)) {
        write_value(buf, std::string(boost::any_cast<const char *>(v)), true);
    } else if (v.type() == typeid(uint8_t)) {
        write_value(buf, boost::any_cast<uint8_t>(v), true);
    } else if (v.type() == typeid(std::vector<uint8_t>)) {
        write_value(buf, boost::any_cast<std::vector<uint8_t>>(v), true);
    } else if (v.type() == typeid(int16_t)) {
        write_value(buf, boost::any_cast<int16_t>(v), true);
    } else if (v.type() == typeid(int32_t)) {
        write_value(buf, boost::any_cast<int32_t>(v), true);
    } else if (v.type() == typeid(int64_t)) {
        write_value(buf, boost::any_cast<int64_t>(v), true);
    } else if (v.type() == typeid(float_t)) {
        write_value(buf, boost::any_cast<float_t>(v), true);
    } else if (v.type() == typeid(double_t)) {
        write_value(buf, boost::any_cast<double_t>(v), true);
    } else {
        LOG_ERROR("unsupported Any type:{}", v.type().name());
        return E_MOTAN_UNSUPPORTED_TYPE;
    }
    return MOTAN_OK;
}

#if (defined(__clang__) && (__GNUC__ > 3)) || (!defined(__clang__) && (__GNUC__ > 6))

// any只能是基础类型，不能嵌套
int breeze::write_value(BytesBuffer *buf, const std::any &v, bool with_type) {
    if (v.type() == typeid(std::nullptr_t)) {
        write_value(buf, nullptr, true);
    } else if (v.type() == typeid(bool)) {
        write_value(buf, std::any_cast<bool>(v), true);
    } else if (v.type() == typeid(std::string)) {
        write_value(buf, std::any_cast<std::string>(v), true);
    } else if (v.type() == typeid(const char *)) {
        write_value(buf, std::string(std::any_cast<const char *>(v)), true);
    } else if (v.type() == typeid(uint8_t)) {
        write_value(buf, std::any_cast<uint8_t>(v), true);
    } else if (v.type() == typeid(std::vector<uint8_t>)) {
        write_value(buf, std::any_cast<std::vector<uint8_t>>(v), true);
    } else if (v.type() == typeid(int16_t)) {
        write_value(buf, std::any_cast<int16_t>(v), true);
    } else if (v.type() == typeid(int32_t)) {
        write_value(buf, std::any_cast<int32_t>(v), true);
    } else if (v.type() == typeid(int64_t)) {
        write_value(buf, std::any_cast<int64_t>(v), true);
    } else if (v.type() == typeid(float_t)) {
        write_value(buf, std::any_cast<float_t>(v), true);
    } else if (v.type() == typeid(double_t)) {
        write_value(buf, std::any_cast<double_t>(v), true);
    } else {
        LOG_ERROR("unsupported Any type:{}", v.type().name());
        return E_MOTAN_UNSUPPORTED_TYPE;
    }
    return MOTAN_OK;
}

#endif

int breeze::write_value(BytesBuffer *buf, const int16_t &v, bool with_type) {
    if (with_type) {
        buf->write_byte(kBreezeInt16Type);
    }
    buf->write_uint16(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const uint16_t &v, bool with_type) {
    if (with_type) {
        buf->write_byte(kBreezeInt16Type);
    }
    buf->write_uint16(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const uint32_t &v, bool with_type) {
    if (with_type) {
        buf->write_byte(kBreezeInt32Type);
    }
    buf->write_zigzag32(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const int32_t &v, bool with_type) {
    if (with_type) {
        if (v >= kBreezeDirectInt32MinValue && v <= kBreezeDirectInt32MaxValue) {
            buf->write_byte(v + kBreezeInt32Zero);
            return MOTAN_OK;
        }
        buf->write_byte(kBreezeInt32Type);
    }
    buf->write_zigzag32(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const int64_t &v, bool with_type) {
    if (with_type) {
        if (v >= kBreezeDirectInt64MinValue && v <= kBreezeDirectInt64MaxValue) {
            buf->write_byte(v + kBreezeInt64Zero);
            return MOTAN_OK;
        }
        buf->write_byte(kBreezeInt64Type);
    }
    buf->write_zigzag64(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const uint64_t &v, bool with_type) {
    if (with_type) {
        buf->write_byte(kBreezeInt64Type);
    }
    buf->write_zigzag64(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const std::string &s, bool with_type) {
    if (with_type) {
        int len = s.length();
        if (len <= kBreezeDirectStringMaxLength) {
            buf->write_byte(len);
            buf->write_bytes(const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(s.data())), len);
            return MOTAN_OK;
        }
        buf->write_byte(kBreezeStringType);
    }
    buf->write_varint(s.length());
    buf->write_bytes(const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(s.data())), s.length());
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const bool &v, bool with_type) {
    if (v) {
        buf->write_byte(kBreezeTrueType);
    } else {
        buf->write_byte(kBreezeFalseType);
    }
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const uint8_t &v, bool with_type) {
    if (with_type) {
        buf->write_byte(kBreezeByteType);
    }
    buf->write_byte(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const std::vector<uint8_t> &b, bool with_type) {
    if (with_type) {
        buf->write_byte(kBreezeBytesType);
    }
    buf->write_uint32(b.size());
    buf->write_bytes(b.data(), b.size());
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const float_t &v, bool with_type) {
    if (with_type) {
        buf->write_byte(kBreezeFloat32Type);
    }
    buf->write_uint32(*(uint32_t *) (&v));
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const double_t &v, bool with_type) {
    if (with_type) {
        buf->write_byte(kBreezeFloat64Type);
    }
    buf->write_uint64(*(uint64_t *) (&v));
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const std::nullptr_t &v, bool with_type) {
    buf->write_byte(kBreezeNullType);
    return MOTAN_OK;
}

// any只能是基础类型，不能嵌套
int breeze::read_value(BytesBuffer *buf, boost::any &v, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeNullType) {
        v = nullptr;
        return MOTAN_OK;
    }
    if (type == kBreezeTrueType) {
        v = true;
        return MOTAN_OK;
    }
    if (type == kBreezeFalseType) {
        v = false;
        return MOTAN_OK;
    }
    if (type == kBreezeByteType) {
        uint8_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_byte err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeBytesType) {
        std::vector<uint8_t> i{};
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_value err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeInt16Type) {
        int16_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
        v = int16_t(i);
        return MOTAN_OK;
    }
    if (type >= kBreezeDirectInt32MinType && type <= kBreezeInt32Type) {
        int32_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
        v = int32_t(i);
        return MOTAN_OK;
    }
    if (type >= kBreezeDirectInt64MinType && type <= kBreezeInt64Type) {
        int64_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        v = int64_t(i);
        return MOTAN_OK;
    }
    if (type == kBreezeFloat32Type) {
        float_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_uint32 err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeFloat64Type) {
        double_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_uint64 err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type <= kBreezeStringType) {
        std::string str;
        if ((err = read_value(buf, str, false, type, ""))) {
            return err;
        }
        v = str;
        return MOTAN_OK;
    }
    LOG_ERROR("unsupported Any type:{}", type);
    return E_MOTAN_UNSUPPORTED_TYPE;
}

#if (defined(__clang__) && (__GNUC__ > 3)) || (!defined(__clang__) && (__GNUC__ > 6))

// any只能是基础类型，不能嵌套
int breeze::read_value(BytesBuffer *buf, std::any &v, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeNullType) {
        v = nullptr;
        return MOTAN_OK;
    }
    if (type == kBreezeTrueType) {
        v = true;
        return MOTAN_OK;
    }
    if (type == kBreezeFalseType) {
        v = false;
        return MOTAN_OK;
    }
    if (type == kBreezeByteType) {
        uint8_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_byte err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeBytesType) {
        std::vector<uint8_t> i{};
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_value err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeInt16Type) {
        int16_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
        v = int16_t(i);
        return MOTAN_OK;
    }
    if (type >= kBreezeDirectInt32MinType && type <= kBreezeInt32Type) {
        int32_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
        v = int32_t(i);
        return MOTAN_OK;
    }
    if (type >= kBreezeDirectInt64MinType && type <= kBreezeInt64Type) {
        int64_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        v = int64_t(i);
        return MOTAN_OK;
    }
    if (type == kBreezeFloat32Type) {
        float_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_uint32 err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeFloat64Type) {
        double_t i;
        if ((err = read_value(buf, i, false, type, ""))) {
            LOG_ERROR("read_uint64 err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type <= kBreezeStringType) {
        std::string str;
        if ((err = read_value(buf, str, false, type, ""))) {
            return err;
        }
        v = str;
        return MOTAN_OK;
    }
    LOG_ERROR("unsupported Any type:{}", type);
    return E_MOTAN_UNSUPPORTED_TYPE;
}

#endif

int breeze::read_value(BytesBuffer *buf, BreezeMessage &res, bool with_type, uint8_t type, const std::string &msg_name) {
    if (with_type && !check_type(buf, kBreezeMessageType)) {
        LOG_ERROR("mismatch type");
        return E_MOTAN_MISMATCH_TYPE;
    }
    return read_message(buf, res);
}

int breeze::read_value(BytesBuffer *buf, std::nullptr_t &res, bool with_type, uint8_t type, const std::string &msg_name) {
    if (with_type && !check_type(buf, kBreezeNullType)) {
        LOG_ERROR("mismatch type");
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, bool &res, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeTrueType) {
        res = true;
        return MOTAN_OK;
    } else if (type == kBreezeFalseType) {
        res = false;
        return MOTAN_OK;
    }
    LOG_ERROR("wrong bool type:{}", type);
    return E_MOTAN_UNSUPPORTED_TYPE;
}

int breeze::read_value(BytesBuffer *buf, uint8_t &res, bool with_type, uint8_t type, const std::string &msg_name) {
    if (with_type && !check_type(buf, kBreezeByteType)) {
        LOG_ERROR("mismatch type");
        return E_MOTAN_MISMATCH_TYPE;
    }
    if (auto err = buf->read_byte(*reinterpret_cast<uint8_t *>(&res))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, std::vector<uint8_t> &res, bool with_type, uint8_t type, const std::string &msg_name) {
    if (with_type && !check_type(buf, kBreezeBytesType)) {
        LOG_ERROR("mismatch type");
        return E_MOTAN_MISMATCH_TYPE;
    }
    return read_bytes(buf, res);
}

int breeze::read_value(BytesBuffer *buf, int16_t &res, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeInt16Type) {
        if ((err = buf->read_uint16(*reinterpret_cast<uint16_t *>(&res)))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
    } else if (type == kBreezeInt32Type) {
        if ((err = buf->read_zigzag32(*reinterpret_cast<uint64_t *>(&res)))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
    } else if (type == kBreezeStringType) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        res = std::strtol(str.data(), nullptr, 10);
    } else if (type == kBreezeInt64Type) {
        if ((err = buf->read_zigzag64(*reinterpret_cast<uint64_t *>(&res)))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt16Type, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, uint16_t &res, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeInt16Type) {
        if ((err = buf->read_uint16(*reinterpret_cast<uint16_t *>(&res)))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
    } else if (type == kBreezeInt32Type) {
        if ((err = buf->read_zigzag32(*reinterpret_cast<uint64_t *>(&res)))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
    } else if (type == kBreezeStringType) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        res = std::strtol(str.data(), nullptr, 10);
    } else if (type == kBreezeInt64Type) {
        if ((err = buf->read_zigzag64(*reinterpret_cast<uint64_t *>(&res)))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt16Type, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, int32_t &res, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type >= kBreezeDirectInt32MinType && type <= kBreezeDirectInt32MaxType) {
        res = type - kBreezeInt32Zero;
        return MOTAN_OK;
    }
    if (type == kBreezeInt32Type) {
        uint64_t temp{};
        if ((err = buf->read_zigzag32(temp))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
        res = temp;
    } else if (type == kBreezeInt16Type) {
        uint16_t temp{};
        if ((err = buf->read_uint16(temp))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
        res = temp;
    } else if (type == kBreezeInt64Type) {
        uint64_t temp{};
        if ((err = buf->read_zigzag64(temp))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        res = temp;
    } else if (type == kBreezeStringType) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        res = std::strtol(str.data(), nullptr, 10);
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt32Type, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, uint32_t &res, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeInt32Type) {
        uint64_t temp{};
        if ((err = buf->read_zigzag32(temp))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
        res = temp;
    } else if (type == kBreezeInt16Type) {
        uint16_t temp{};
        if ((err = buf->read_uint16(temp))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
        res = temp;
    } else if (type == kBreezeInt64Type) {
        uint64_t temp{};
        if ((err = buf->read_zigzag64(temp))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        res = temp;
    } else if (type == kBreezeStringType) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        res = std::strtol(str.data(), nullptr, 10);
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt32Type, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, int64_t &res, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type >= kBreezeDirectInt64MinType && type <= kBreezeDirectInt64MaxType) {
        res = type - kBreezeInt64Zero;
    } else if (type == kBreezeInt64Type) {
        uint64_t temp{};
        if ((err = buf->read_zigzag64(temp))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        res = temp;
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt64Type, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, uint64_t &res, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeInt64Type) {
        uint64_t temp{};
        if ((err = buf->read_zigzag64(temp))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        res = temp;
    } else if (type == kBreezeInt16Type) {
        uint16_t temp{};
        if ((err = buf->read_uint16(temp))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
        res = temp;
    } else if (type == kBreezeInt32Type) {
        uint64_t temp{};
        if ((err = buf->read_zigzag32(temp))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
        res = temp;
    } else if (type == kBreezeStringType) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        res = std::strtoll(str.data(), nullptr, 10);
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt64Type, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, float_t &res, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeFloat32Type) {
        if ((err = buf->read_uint32(*reinterpret_cast<uint32_t *>(&res)))) {
            LOG_ERROR("read_uint32 err:{}", err);
            return err;
        }
    } else if (type == kBreezeStringType) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        res = std::strtof(str.data(), nullptr);
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeFloat32Type, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, double_t &i, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeFloat64Type) {
        if ((err = buf->read_uint64(*reinterpret_cast<uint64_t *>(&i)))) {
            LOG_ERROR("read_uint64 err:{}", err);
            return err;
        }
    } else if (type == kBreezeStringType) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        i = std::strtod(str.data(), nullptr);
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeFloat64Type, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, std::string &res, bool with_type, uint8_t type, const std::string &msg_name) {
    int err{};
    if (with_type && (err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type <= kBreezeStringType) {
        if (type == kBreezeStringType) {
            return read_string(buf, res);
        }
        BytesBuffer b{};
        if ((err = buf->next(type, b))) {
            LOG_ERROR("read next err:{}", err);
            return err;
        }
        res = std::string(reinterpret_cast<char *>(b.buffer_), b.write_pos_);
        return MOTAN_OK;
    }
    //TODO 兼容模式
    return E_MOTAN_MISMATCH_TYPE;
}

int breeze::read_message(BytesBuffer *buf, BreezeMessage &v) {
    std::string name{};
    int err{};
    if ((err = read_value(buf, name, true, 0, ""))) {
        LOG_ERROR("read_value err:{}", err);
        return err;
    }
    if (v.get_name() != name) {
        if (typeid(v) != typeid(GenericMessage)) {
            LOG_ERROR("mismatch message type. expect:{}, real:{}", v.get_name().data(), name.data());
            return E_MOTAN_MISMATCH_TYPE;
        }
        v.set_name(name);
    }
    if ((err = v.read_from(buf))) {
        LOG_ERROR("read_from err:{}", err);
        return err;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, BreezeEnum &v, bool with_type, uint8_t type, const std::string &msg_name) {
    if (with_type && !check_type(buf, kBreezeMessageType)) {
        LOG_ERROR("mismatch type");
        return E_MOTAN_MISMATCH_TYPE;
    }
    return read_enum(buf, v);
}

int breeze::read_enum(BytesBuffer *buf, BreezeEnum &v) {
    std::string name{};
    int err{};
    if ((err = read_value(buf, name, true, 0, ""))) {
        LOG_ERROR("read_value err:{}", err);
        return err;
    }
    if (v.get_name() != name) {
        LOG_ERROR("mismatch message type. expect:{}, real:{}", v.get_name().data(), name.data());
        return E_MOTAN_MISMATCH_TYPE;
    }
    if ((err = v.read_enum(buf))) {
        LOG_ERROR("read_from err:{}", err);
        return err;
    }
    return MOTAN_OK;
}

int breeze::read_string(BytesBuffer *buf, std::string &v) {
    uint64_t size = 0;
    int err{};
    if ((err = buf->read_varint(size))) {
        LOG_ERROR("read_zigzag32 err:{}", err);
        return err;
    }
    BytesBuffer b{};
    if ((err = buf->next(size, b))) {
        LOG_ERROR("read next err:{}", err);
        return err;
    }
    v = std::string(reinterpret_cast<char *>(b.buffer_), b.write_pos_);
    return MOTAN_OK;
}

int breeze::read_bytes(BytesBuffer *buf, std::vector<uint8_t> &v) {
    uint32_t size = 0;
    int err{};
    if ((err = buf->read_uint32(size))) {
        LOG_ERROR("read_uint32 err:{}", err);
        return err;
    }
    BytesBuffer b{};
    if ((err = buf->next(size, b))) {
        LOG_ERROR("read next err:{}", err);
        return err;
    }
    for (auto x = 0; x < b.write_pos_; x++) {
        v.push_back(b.buffer_[x]);
    }
    return MOTAN_OK;
}

bool breeze::check_type(BytesBuffer *buf, uint8_t expect) {
    uint8_t type{};
    if (auto err = buf->read_byte(type)) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    return type == expect;
}


std::string GenericMessage::get_name() const {
    return name_;
}

std::string GenericMessage::get_alias() {
    return alias_;
}

int GenericMessage::read_from(BytesBuffer *buf) {
    return breeze::read_message_by_field(buf, [this](BytesBuffer *buf, uint64_t index) {
        boost::any temp;
        if (auto err = breeze::read_value(buf, temp, true, 0, "")) {
            return err;
        }
        this->fields_[index] = temp;
        return MOTAN_OK;
    });
}

int GenericMessage::write_to(BytesBuffer *buf) const {
    return breeze::write_message(buf, this->get_name(), [buf, this]() {
        for (auto &x: this->fields_) {
            //type()无类型，无法作为switch参数
            if (x.second.type() == typeid(std::nullptr_t)) {
                breeze::write_message_field(buf, x.first, nullptr);
            } else if (x.second.type() == typeid(bool)) {
                breeze::write_message_field(buf, x.first, boost::any_cast<bool>(x.second));
            } else if (x.second.type() == typeid(std::string)) {
                breeze::write_message_field(buf, x.first, boost::any_cast<std::string>(x.second));
            } else if (x.second.type() == typeid(const char *)) {
                breeze::write_message_field(buf, x.first, std::string(boost::any_cast<const char *>(x.second)));
            } else if (x.second.type() == typeid(uint8_t)) {
                breeze::write_message_field(buf, x.first, boost::any_cast<uint8_t>(x.second));
            } else if (x.second.type() == typeid(std::vector<uint8_t>)) {
                breeze::write_message_field(buf, x.first, boost::any_cast<std::vector<uint8_t>>(x.second));
            } else if (x.second.type() == typeid(int16_t)) {
                breeze::write_message_field(buf, x.first, boost::any_cast<int16_t>(x.second));
            } else if (x.second.type() == typeid(int32_t)) {
                breeze::write_message_field(buf, x.first, boost::any_cast<int32_t>(x.second));
            } else if (x.second.type() == typeid(int64_t)) {
                breeze::write_message_field(buf, x.first, boost::any_cast<int64_t>(x.second));
            } else if (x.second.type() == typeid(float_t)) {
                breeze::write_message_field(buf, x.first, boost::any_cast<float_t>(x.second));
            } else if (x.second.type() == typeid(double_t)) {
                breeze::write_message_field(buf, x.first, boost::any_cast<double_t>(x.second));
            } else if (x.second.type() == typeid(GenericMessage)) {
                breeze::write_message_field(buf, x.first, boost::any_cast<GenericMessage>(x.second));
            } else if (x.second.type() == typeid(std::vector<boost::any>)) {
                breeze::write_message_field(buf, x.first, boost::any_cast<std::vector<boost::any>>(x.second));
            } else {
                LOG_ERROR("unsupported type:{}", x.second.type().name());
                return E_MOTAN_UNSUPPORTED_TYPE;
            }
        }
        return MOTAN_OK;
    });
}

std::shared_ptr<BreezeSchema> GenericMessage::get_schema() {
    return this->schema_;
}

GenericMessage::GenericMessage(std::string name, std::string alias) : name_(std::move(name)), alias_(std::move(alias)) {}

boost::any GenericMessage::get_field_by_index(const int32_t &index) {
    auto iter = this->fields_.find(index);
    if (iter != this->fields_.end()) {
        return this->fields_[index];
    }
    return nullptr;
}

boost::any GenericMessage::get_field_by_name(const std::string &name) {
    if (this->schema_ == nullptr) {
        LOG_WARN("schema is null, name:{}", name);
        return boost::any(nullptr);
    }
    auto field = this->schema_->get_field_by_name(name);
    if (field != nullptr) {
        return this->fields_[field->index_];
    }
    return nullptr;
}

void GenericMessage::put_field(const int32_t &index, const boost::any &field) {
    if (index > -1 && field.type() != typeid(std::nullptr_t)) {
        this->fields_[index] = field;
    }
}

void GenericMessage::set_name(const std::string &name) {
    this->name_ = name;
}
