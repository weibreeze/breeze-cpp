//
// Created by zhachen on 2019-05-23.
//
#include "breeze.h"

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

int breeze::write_value(BytesBuffer *buf, const BreezeMessage &v) {
    return v.write_to(buf);
}

int breeze::write_value(BytesBuffer *buf, const int16_t &v) {
    buf->write_byte(kBreezeInt16);
    buf->write_uint16(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const uint16_t &v) {
    buf->write_byte(kBreezeInt16);
    buf->write_uint16(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const uint32_t &v) {
    buf->write_byte(kBreezeInt32);
    buf->write_zigzag32(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const int32_t &v) {
    buf->write_byte(kBreezeInt32);
    buf->write_zigzag32(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const int64_t &v) {
    buf->write_byte(kBreezeInt64);
    buf->write_zigzag64(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const uint64_t &v) {
    buf->write_byte(kBreezeInt64);
    buf->write_zigzag64(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const std::string &s) {
    buf->write_byte(kBreezeString);
    buf->write_zigzag32(s.length());
    buf->write_bytes(const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(s.data())), s.length());
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const bool &v) {
    if (v) {
        buf->write_byte(kBreezeTrue);
    } else {
        buf->write_byte(kBreezeFalse);
    }
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const uint8_t &v) {
    buf->write_byte(kBreezeByte);
    buf->write_byte(v);
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const std::vector<uint8_t> &b) {
    buf->write_byte(kBreezeBytes);
    buf->write_uint32(b.size());
    buf->write_bytes(b.data(), b.size());
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const float_t &v) {
    buf->write_byte(kBreezeFloat32);
    buf->write_uint32(*(uint32_t *) (&v));
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const double_t &v) {
    buf->write_byte(kBreezeFloat64);
    buf->write_uint64(*(uint64_t *) (&v));
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const std::any &v) {
    if (v.type() == typeid(std::nullptr_t)) {
        write_value(buf, nullptr);
    } else if (v.type() == typeid(bool)) {
        write_value(buf, std::any_cast<bool>(v));
    } else if (v.type() == typeid(std::string)) {
        write_value(buf, std::any_cast<std::string>(v));
    } else if (v.type() == typeid(uint8_t)) {
        write_value(buf, std::any_cast<uint8_t>(v));
    } else if (v.type() == typeid(std::vector<uint8_t>)) {
        write_value(buf, std::any_cast<std::vector<uint8_t>>(v));
    } else if (v.type() == typeid(int16_t)) {
        write_value(buf, std::any_cast<int16_t>(v));
    } else if (v.type() == typeid(int32_t)) {
        write_value(buf, std::any_cast<int32_t>(v));
    } else if (v.type() == typeid(int64_t)) {
        write_value(buf, std::any_cast<int64_t>(v));
    } else if (v.type() == typeid(float_t)) {
        write_value(buf, std::any_cast<float_t>(v));
    } else if (v.type() == typeid(double_t)) {
        write_value(buf, std::any_cast<double_t>(v));
    } else if (v.type() == typeid(GenericMessage)) {
        write_value(buf, std::any_cast<GenericMessage>(v));
    } else if (v.type() == typeid(std::vector<std::any>)) {
        write_value(buf, std::any_cast<std::vector<std::any>>(v));
    } else if (v.type() == typeid(std::unordered_map<BREEZE_VARIANT, std::any>)) {
        write_value(buf, std::any_cast<std::unordered_map<BREEZE_VARIANT, std::any>>(v));
    } else {
        LOG_ERROR("unsupported type:{}", v.type().name());
        return E_MOTAN_UNSUPPORTED_TYPE;
    }
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const BREEZE_VARIANT &v) {
    switch (v.index()) {
        case 0:
            write_value(buf, std::get<bool>(v));
            break;
        case 1:
            write_value(buf, std::get<uint8_t>(v));
            break;
        case 2:
            write_value(buf, std::get<int16_t>(v));
            break;
        case 3:
            write_value(buf, std::get<int32_t>(v));
            break;
        case 4:
            write_value(buf, std::get<int64_t>(v));
            break;
        case 5:
            write_value(buf, std::get<float_t>(v));
            break;
        case 6:
            write_value(buf, std::get<double_t>(v));
            break;
        case 7:
            write_value(buf, std::get<std::string>(v));
            break;
        default:
            LOG_ERROR("unsupported type:{}", v.index());
            return E_MOTAN_UNSUPPORTED_TYPE;
    }
    return MOTAN_OK;
}

int breeze::write_value(BytesBuffer *buf, const std::nullptr_t &v) {
    buf->write_byte(kBreezeNull);
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, BreezeMessage &v) {
    if (!check_type(buf, kBreezeMessage)) {
        LOG_ERROR("mismatch type");
        return E_MOTAN_MISMATCH_TYPE;
    }
    return read_message(buf, v);
}

int breeze::read_value(BytesBuffer *buf, std::nullptr_t &i) {
    if (!check_type(buf, kBreezeNull)) {
        LOG_ERROR("mismatch type");
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, bool &i) {
    uint8_t type;
    if (auto err = buf->read_byte(type)) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeTrue) {
        i = true;
        return MOTAN_OK;
    } else if (type == kBreezeFalse) {
        i = false;
        return MOTAN_OK;
    }
    LOG_ERROR("wrong bool type:{}", type);
    return E_MOTAN_UNSUPPORTED_TYPE;
}

int breeze::read_value(BytesBuffer *buf, uint8_t &i) {
    if (!check_type(buf, kBreezeByte)) {
        LOG_ERROR("mismatch type");
        return E_MOTAN_MISMATCH_TYPE;
    }
    if (auto err = buf->read_byte(*reinterpret_cast<uint8_t *>(&i))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, std::vector<uint8_t> &i) {
    if (!check_type(buf, kBreezeBytes)) {
        LOG_ERROR("mismatch type");
        return E_MOTAN_MISMATCH_TYPE;
    }
    return read_bytes(buf, i);
}

int breeze::read_value(BytesBuffer *buf, int16_t &i) {
    uint8_t type;
    int err;
    if ((err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeInt16) {
        if ((err = buf->read_uint16(*reinterpret_cast<uint16_t *>(&i)))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
    } else if (type == kBreezeInt32) {
        if ((err = buf->read_zigzag32(*reinterpret_cast<uint64_t *>(&i)))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
    } else if (type == kBreezeString) {
        std::string str;

        if ((err = read_string(buf, str))) {
            return err;
        }
        i = std::strtol(str.data(), nullptr, 10);
    } else if (type == kBreezeInt64) {
        if ((err = buf->read_zigzag64(*reinterpret_cast<uint64_t *>(&i)))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt16, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, uint16_t &i) {
    uint8_t type;
    int err;
    if ((err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeInt16) {
        if ((err = buf->read_uint16(*reinterpret_cast<uint16_t *>(&i)))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
    } else if (type == kBreezeInt32) {
        if ((err = buf->read_zigzag32(*reinterpret_cast<uint64_t *>(&i)))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
    } else if (type == kBreezeString) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        i = std::strtol(str.data(), nullptr, 10);
    } else if (type == kBreezeInt64) {
        if ((err = buf->read_zigzag64(*reinterpret_cast<uint64_t *>(&i)))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt16, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, int32_t &i) {
    uint8_t type;
    int err;
    if ((err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeInt32) {
        uint64_t temp{};
        if ((err = buf->read_zigzag32(temp))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeInt16) {
        uint16_t temp{};
        if ((err = buf->read_uint16(temp))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeInt64) {
        uint64_t temp{};
        if ((err = buf->read_zigzag64(temp))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeString) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        i = std::strtol(str.data(), nullptr, 10);
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt32, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, uint32_t &i) {
    uint8_t type;
    int err;
    if ((err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeInt32) {
        uint64_t temp{};
        if ((err = buf->read_zigzag32(temp))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeInt16) {
        uint16_t temp{};
        if ((err = buf->read_uint16(temp))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeInt64) {
        uint64_t temp{};
        if ((err = buf->read_zigzag64(temp))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeString) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        i = std::strtol(str.data(), nullptr, 10);
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt32, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, int64_t &i) {
    uint8_t type;
    int err;
    if ((err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeInt64) {
        uint64_t temp{};
        if ((err = buf->read_zigzag64(temp))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeInt16) {
        uint16_t temp{};
        if ((err = buf->read_uint16(temp))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeInt32) {
        uint64_t temp{};
        if ((err = buf->read_zigzag32(temp))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeString) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        i = std::strtoll(str.data(), nullptr, 10);
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt64, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, uint64_t &i) {
    uint8_t type;
    int err;
    if ((err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeInt64) {
        uint64_t temp{};
        if ((err = buf->read_zigzag64(temp))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeInt16) {
        uint16_t temp{};
        if ((err = buf->read_uint16(temp))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeInt32) {
        uint64_t temp{};
        if ((err = buf->read_zigzag32(temp))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
        i = temp;
    } else if (type == kBreezeString) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        i = std::strtoll(str.data(), nullptr, 10);
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeInt64, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, float_t &i) {
    uint8_t type;
    int err;
    if ((err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeFloat32) {
        if ((err = buf->read_uint32(*reinterpret_cast<uint32_t *>(&i)))) {
            LOG_ERROR("read_uint32 err:{}", err);
            return err;
        }
    } else if (type == kBreezeString) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        i = std::strtof(str.data(), nullptr);
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeFloat32, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, double_t &i) {
    uint8_t type;
    int err;
    if ((err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeFloat64) {
        if ((err = buf->read_uint64(*reinterpret_cast<uint64_t *>(&i)))) {
            LOG_ERROR("read_uint64 err:{}", err);
            return err;
        }
    } else if (type == kBreezeString) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        i = std::strtod(str.data(), nullptr);
    } else {
        LOG_ERROR("mismatch type, expect:{}, real:{}", kBreezeFloat64, type);
        return E_MOTAN_MISMATCH_TYPE;
    }
    return MOTAN_OK;
}

int breeze::read_value(BytesBuffer *buf, std::string &v) {
    if (!check_type(buf, kBreezeString)) {
        LOG_ERROR("mismatch type");
        return E_MOTAN_MISMATCH_TYPE;
    }
    return read_string(buf, v);
}

int breeze::read_message(BytesBuffer *buf, BreezeMessage &v) {
    std::string name{};
    int err;
    if ((err = read_value(buf, name))) {
        LOG_ERROR("read_value err:{}", err);
        return err;
    }
    if (v.get_name() != name && v.get_alias() != name) {
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

int breeze::read_string(BytesBuffer *buf, std::string &v) {
    uint64_t size = 0;
    int err;
    if ((err = buf->read_zigzag32(size))) {
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
    int err;
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
    uint8_t type;
    if (auto err = buf->read_byte(type)) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    return type == expect;
}

int breeze::read_value(BytesBuffer *buf, std::any &v) {
    int err;
    uint8_t type;
    if ((err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeNull) {
        v = nullptr;
        return MOTAN_OK;
    }
    if (type == kBreezeTrue) {
        v = true;
        return MOTAN_OK;
    }
    if (type == kBreezeFalse) {
        v = false;
        return MOTAN_OK;
    }
    if (type == kBreezeByte) {
        uint8_t i;
        if ((err = buf->read_byte(i))) {
            LOG_ERROR("read_byte err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeBytes) {
        std::vector<uint8_t> i{};
        if ((err = read_bytes(buf, i))) {
            LOG_ERROR("read_value err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeInt16) {
        uint16_t i;
        if ((err = buf->read_uint16(i))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
        v = int16_t(i);
        return MOTAN_OK;
    }
    if (type == kBreezeInt32) {
        uint64_t i;
        if ((err = buf->read_zigzag32(i))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
        v = int32_t(i);
        return MOTAN_OK;
    }
    if (type == kBreezeInt64) {
        uint64_t i;
        if ((err = buf->read_zigzag64(i))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        v = int64_t(i);
        return MOTAN_OK;
    }
    if (type == kBreezeFloat32) {
        float_t i;
        if ((err = buf->read_uint32(*reinterpret_cast<uint32_t *>(&i)))) {
            LOG_ERROR("read_uint32 err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeFloat64) {
        double_t i;
        if ((err = buf->read_uint64(*reinterpret_cast<uint64_t *>(&i)))) {
            LOG_ERROR("read_uint64 err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeString) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        v = str;
        return MOTAN_OK;
    }
    if (type == kBreezeMessage) {
        GenericMessage msg;
        if ((err = read_message(buf, msg))) {
            return err;
        }
        v = msg;
        return MOTAN_OK;
    }
    if (type == kBreezeArray) {
        std::vector<std::any> arr;
        if ((err = ReadArray(buf, arr))) {
            return err;
        }
        v = arr;
        return MOTAN_OK;
    }
    if (type == kBreezeMap) {
        std::unordered_map<BREEZE_VARIANT, std::any> map;
        if ((err = read_map(buf, map))) {
            return err;
        }
        v = map;
        return MOTAN_OK;
    }
    LOG_ERROR("unsupported type:{}", type);
    return E_MOTAN_UNSUPPORTED_TYPE;
}

int breeze::read_value(BytesBuffer *buf, BREEZE_VARIANT &v) {
    int err;
    uint8_t type;
    if ((err = buf->read_byte(type))) {
        LOG_ERROR("read_byte err:{}", err);
        return err;
    }
    if (type == kBreezeInt16) {
        uint16_t i;
        if ((err = buf->read_uint16(i))) {
            LOG_ERROR("read_uint16 err:{}", err);
            return err;
        }
        v = int16_t(i);
        return MOTAN_OK;
    }
    if (type == kBreezeInt32) {
        uint64_t i;
        if ((err = buf->read_zigzag32(i))) {
            LOG_ERROR("read_zigzag32 err:{}", err);
            return err;
        }
        v = int32_t(i);
        return MOTAN_OK;
    }
    if (type == kBreezeInt64) {
        uint64_t i;
        if ((err = buf->read_zigzag64(i))) {
            LOG_ERROR("read_zigzag64 err:{}", err);
            return err;
        }
        v = int64_t(i);
        return MOTAN_OK;
    }
    if (type == kBreezeFloat32) {
        float_t i;
        if ((err = buf->read_uint32(*reinterpret_cast<uint32_t *>(&i)))) {
            LOG_ERROR("read_uint32 err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeFloat64) {
        double_t i;
        if ((err = buf->read_uint64(*reinterpret_cast<uint64_t *>(&i)))) {
            LOG_ERROR("read_uint64 err:{}", err);
            return err;
        }
        v = i;
        return MOTAN_OK;
    }
    if (type == kBreezeString) {
        std::string str;
        if ((err = read_string(buf, str))) {
            return err;
        }
        v = str;
        return MOTAN_OK;
    }
    LOG_ERROR("unsupported type:{}", type);
    return E_MOTAN_UNSUPPORTED_TYPE;
}

std::string GenericMessage::get_name() const {
    return name_;
}

std::string GenericMessage::get_alias() {
    return alias_;
}

int GenericMessage::read_from(BytesBuffer *buf) {
    return breeze::read_message_by_field(buf, [this](BytesBuffer *buf, uint64_t index) {
        std::any temp;
        if (auto err = breeze::read_value(buf, temp)) {
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
                breeze::write_message_field(buf, x.first, std::any_cast<bool>(x.second));
            } else if (x.second.type() == typeid(std::string)) {
                breeze::write_message_field(buf, x.first, std::any_cast<std::string>(x.second));
            } else if (x.second.type() == typeid(const char *)) {
                breeze::write_message_field(buf, x.first, std::string(std::any_cast<const char *>(x.second)));
            } else if (x.second.type() == typeid(uint8_t)) {
                breeze::write_message_field(buf, x.first, std::any_cast<uint8_t>(x.second));
            } else if (x.second.type() == typeid(std::vector<uint8_t>)) {
                breeze::write_message_field(buf, x.first, std::any_cast<std::vector<uint8_t>>(x.second));
            } else if (x.second.type() == typeid(int16_t)) {
                breeze::write_message_field(buf, x.first, std::any_cast<int16_t>(x.second));
            } else if (x.second.type() == typeid(int32_t)) {
                breeze::write_message_field(buf, x.first, std::any_cast<int32_t>(x.second));
            } else if (x.second.type() == typeid(int64_t)) {
                breeze::write_message_field(buf, x.first, std::any_cast<int64_t>(x.second));
            } else if (x.second.type() == typeid(float_t)) {
                breeze::write_message_field(buf, x.first, std::any_cast<float_t>(x.second));
            } else if (x.second.type() == typeid(double_t)) {
                breeze::write_message_field(buf, x.first, std::any_cast<double_t>(x.second));
            } else if (x.second.type() == typeid(GenericMessage)) {
                breeze::write_message_field(buf, x.first, std::any_cast<GenericMessage>(x.second));
            } else if (x.second.type() == typeid(std::vector<std::any>)) {
                breeze::write_message_field(buf, x.first, std::any_cast<std::vector<std::any>>(x.second));
            } else if (x.second.type() == typeid(std::unordered_map<BREEZE_VARIANT, std::any>)) {
                breeze::write_message_field(buf, x.first, std::any_cast<std::unordered_map<BREEZE_VARIANT, std::any>>(x.second));
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

std::any GenericMessage::get_field_by_index(const int32_t &index) {
    auto iter = this->fields_.find(index);
    if (iter != this->fields_.end()) {
        return this->fields_[index];
    }
    return nullptr;
}

std::any GenericMessage::get_field_by_name(const std::string &name) {
    if (this->schema_ == nullptr) {
        LOG_WARN("schema is null, name:{}", name);
        return std::any(nullptr);
    }
    auto field = this->schema_->get_field_by_name(name);
    if (field != nullptr) {
        return this->fields_[field->index_];
    }
    return nullptr;
}

void GenericMessage::put_field(const int32_t &index, const std::any &field) {
    if (index > -1 && field.type() != typeid(std::nullptr_t)) {
        this->fields_[index] = field;
    }
}

void GenericMessage::set_name(const std::string &name) {
    this->name_ = name;
}
