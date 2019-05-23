//
// Created by zhachen on 2019-05-23.
//

#include "buffer.h"

void BytesBuffer::dump() {
    fprintf(stderr, "read_only: %s,", read_only_ ? "true" : "false");
    fprintf(stderr, "write_pos: %d,", write_pos_);
    fprintf(stderr, "read_post: %d,", read_pos_);
    fprintf(stderr, "capacity: %zu,", capacity_);
    fprintf(stderr, "buffer: %s\n", buffer_);
}

BytesBuffer *new_bytes_buffer(const size_t &capacity, const ByteOrder &order) {
    auto mb = new BytesBuffer;
    mb->buffer_ = new uint8_t[capacity * sizeof(uint8_t)];
    if (mb->buffer_ == nullptr) {
        delete mb;
        die_bytes_buffer("Out of memory");
    }
    mb->read_only_ = 0;
    mb->order_ = order;
    mb->read_pos_ = 0;
    mb->write_pos_ = 0;
    mb->capacity_ = capacity;
    return mb;
}

BytesBuffer *new_bytes_buffer_from_bytes(const uint8_t *raw_bytes, const size_t &len, const ByteOrder &order,
                                         const uint8_t &read_only) {
    auto mb = new BytesBuffer;
    if (!read_only) {
        mb->buffer_ = new uint8_t[len * sizeof(uint8_t)];
        if (mb->buffer_ == nullptr) {
            delete mb;
            die_bytes_buffer("Out of memory");
        }
        memcpy(mb->buffer_, raw_bytes, len);
    } else {
        mb->buffer_ = (uint8_t *) raw_bytes;
    }
    mb->read_only_ = read_only;
    mb->order_ = order;
    mb->read_pos_ = 0;
    mb->write_pos_ = uint32_t(len);
    mb->capacity_ = len;
    return mb;
}

void BytesBuffer::free_bytes_buffer() {
    if (buffer_ != nullptr) {
        if (!read_only_) {
            delete[] buffer_;
        }
        buffer_ = nullptr;
    }
    delete this;
}

void BytesBuffer::write_bytes(const uint8_t *bytes, const int &len) {
    assert(!read_only_);
    if (capacity_ < write_pos_ + len) {
        grow_buffer(uint32_t(len));
    }
    memcpy((void *) (buffer_ + write_pos_), (void *) bytes, uint32_t(len));
    write_pos_ += len;
}

void BytesBuffer::grow_buffer(const size_t &n) {
    assert(!read_only_);
    size_t new_cap = 2 * capacity_ + n;
    auto new_buffer = new uint8_t[new_cap];
    memcpy(new_buffer, buffer_, capacity_);
    delete[] buffer_;
    buffer_ = new_buffer;
    capacity_ = new_cap;
}

void BytesBuffer::write_byte(const uint8_t &u) {
    assert(!read_only_);
    if (capacity_ < write_pos_ + 1) {
        grow_buffer(1);
    }
    buffer_[write_pos_] = u;
    write_pos_++;
}

void BytesBuffer::write_uint16(const uint16_t &u) {
    assert(!read_only_);
    if (capacity_ < write_pos_ + 2) {
        grow_buffer(2);
    }
    if (order_ == kBigEndian) {
        big_endian_write_uint16(buffer_ + write_pos_, u);
    } else {
        little_endian_write_uint16(buffer_ + write_pos_, u);
    }
    write_pos_ += 2;
}

void BytesBuffer::write_uint32(const uint32_t &u) {
    assert(!read_only_);
    if (capacity_ < write_pos_ + 4) {
        grow_buffer(4);
    }
    if (order_ == kBigEndian) {
        big_endian_write_uint32(buffer_ + write_pos_, u);
    } else {
        little_endian_write_uint32(buffer_ + write_pos_, u);
    }
    write_pos_ += 4;
}

void BytesBuffer::write_uint64(const uint64_t &u) {
    assert(!read_only_);
    if (capacity_ < write_pos_ + 8) {
        grow_buffer(8);
    }
    if (order_ == kBigEndian) {
        big_endian_write_uint64(buffer_ + write_pos_, u);
    } else {
        little_endian_write_uint64(buffer_ + write_pos_, u);
    }
    write_pos_ += 8;
}

int BytesBuffer::write_varint(uint64_t u) {
    assert(!read_only_);
    int l = 0;
    for (; u >= 1 << 7;) {
        write_byte((uint8_t) ((u & 0x7f) | 0x80));
        u >>= 7;
        l++;
    }
    write_byte((uint8_t) u);
    return l + 1;
}

void BytesBuffer::set_write_pos(const uint32_t &pos) {
    assert(!read_only_);
    if (capacity_ < pos) {
        grow_buffer(pos - capacity_);
    }
    write_pos_ = pos;
}

void BytesBuffer::set_read_pos(const uint32_t &pos) {
    read_pos_ = pos;
}

int BytesBuffer::length() {
    return write_pos_;
}

int BytesBuffer::remain() {
    return write_pos_ - read_pos_;
}

int BytesBuffer::next(const int &n, BytesBuffer &ret) {
    auto m = remain();
    if (n > m) {
        return E_MOTAN_BUFFER_NOT_ENOUGH;
    }
    ret.buffer_ = buffer_ + read_pos_;
    ret.write_pos_ = uint32_t(n);
    read_pos_ += n;
    return MOTAN_OK;
}

void BytesBuffer::reset() {
    read_pos_ = 0;
    write_pos_ = 0;
}

int BytesBuffer::read_bytes(uint8_t *bs, const int &len) {
    if (remain() < len) {
        return E_MOTAN_BUFFER_NOT_ENOUGH;
    }
    memcpy((void *) bs, (void *) (buffer_ + read_pos_), uint32_t(len));
    read_pos_ += len;
    return MOTAN_OK;
}

int BytesBuffer::read_byte(uint8_t &u) {
    if (remain() < 1) {
        return E_MOTAN_BUFFER_NOT_ENOUGH;
    }
    u = buffer_[read_pos_];
    read_pos_++;
    return MOTAN_OK;
}

int BytesBuffer::read_uint16(uint16_t &u) {
    if (remain() < 2) {
        return E_MOTAN_BUFFER_NOT_ENOUGH;
    }
    if (order_ == kBigEndian) {
        u = big_endian_read_uint16(buffer_ + read_pos_);
    } else {
        u = little_endian_read_uint16(buffer_ + read_pos_);
    }
    read_pos_ += 2;
    return MOTAN_OK;
}

int BytesBuffer::read_uint32(uint32_t &u) {
    if (remain() < 4) {
        return E_MOTAN_BUFFER_NOT_ENOUGH;
    }
    if (order_ == kBigEndian) {
        u = big_endian_read_uint32(buffer_ + read_pos_);
    } else {
        u = little_endian_read_uint32(buffer_ + read_pos_);
    }
    read_pos_ += 4;
    return MOTAN_OK;
}

int BytesBuffer::read_uint64(uint64_t &u) {
    if (remain() < 8) {
        return E_MOTAN_BUFFER_NOT_ENOUGH;
    }
    if (order_ == kBigEndian) {
        u = big_endian_read_uint64(buffer_ + read_pos_);
    } else {
        u = little_endian_read_uint64(buffer_ + read_pos_);
    }
    read_pos_ += 8;
    return MOTAN_OK;
}

int BytesBuffer::read_varint(uint64_t &u) {
    uint64_t r = 0;
    for (int shift = 0; shift < 64; shift += 7) {
        uint8_t b;
        int err = read_byte(b);
        if (err != MOTAN_OK) {
            return err;
        }
        if ((b & 0x80) != 0x80) {
            r |= (uint64_t) b << shift;
            u = r;
            return MOTAN_OK;
        }
        r |= (uint64_t) (b & 0x7f) << shift;
    }
    return E_MOTAN_OVERFLOW;
}

int BytesBuffer::write_zigzag32(const uint32_t &v) {
    write_varint(uint64_t((v << 1) ^ uint32_t((int32_t(v) >> 31))));
    return 0;
}

int BytesBuffer::write_zigzag64(const uint64_t &v) {
    write_varint(uint64_t((v << 1) ^ uint64_t((int64_t(v) >> 63))));
    return 0;
}

int BytesBuffer::read_zigzag32(uint64_t &v) {
    if (read_varint(v)) {
        return -1;
    }
    v = uint64_t((uint32_t(v) >> 1) ^ uint32_t(-int32_t(v & 1)));
    return 0;
}

int BytesBuffer::read_zigzag64(uint64_t &v) {
    if (read_varint(v)) {
        return -1;
    }
    v = (v >> 1) ^ uint64_t(-int64_t(v & 1));
    return 0;
}

void die_bytes_buffer(const char *fmt, ...) {
    va_list arg;
    va_start(arg, fmt);
    vfprintf(stderr, fmt, arg);
    va_end(arg);
    fprintf(stderr, "\n");
    exit(-1);
}

void dump_bytes_buffer(const uint8_t *bs, const int &len) {
    for (int i = 0; i < len; i++) {
        printf("%02x", (uint8_t) bs[i]);
    }
    printf("\n");
}

uint64_t zigzag_encode(int64_t u) {
    return (u << 1) ^ ((uint64_t) u >> 63);
}

int64_t zigzag_decode(uint64_t n) {
    return (n >> 1) ^ -((int64_t) n & 1);
}

void big_endian_write_uint16(uint8_t *buffer, uint16_t n) {
    buffer[0] = (uint8_t) (n >> 8);
    buffer[1] = (uint8_t) n;

}

void big_endian_write_uint32(uint8_t *buffer, uint32_t n) {
    buffer[0] = (uint8_t) (n >> 24);
    buffer[1] = (uint8_t) (n >> 16);
    buffer[2] = (uint8_t) (n >> 8);
    buffer[3] = (uint8_t) n;
}

void big_endian_write_uint64(uint8_t *buffer, uint64_t n) {
    buffer[0] = (uint8_t) (n >> 56);
    buffer[1] = (uint8_t) (n >> 48);
    buffer[2] = (uint8_t) (n >> 40);
    buffer[3] = (uint8_t) (n >> 32);
    buffer[4] = (uint8_t) (n >> 24);
    buffer[5] = (uint8_t) (n >> 16);
    buffer[6] = (uint8_t) (n >> 8);
    buffer[7] = (uint8_t) n;
}

uint16_t big_endian_read_uint16(const uint8_t *buffer) {
    return ((uint16_t) buffer[0] << 8) | (uint16_t) buffer[1];
}

uint32_t big_endian_read_uint32(const uint8_t *buffer) {
    return ((uint32_t) buffer[0] << 24)
           | ((uint32_t) buffer[1] << 16)
           | ((uint32_t) buffer[2] << 8)
           | (uint32_t) buffer[3];
}

uint64_t big_endian_read_uint64(const uint8_t *buffer) {
    return ((uint64_t) buffer[0] << 56)
           | ((uint64_t) buffer[1] << 48)
           | ((uint64_t) buffer[2] << 40)
           | ((uint64_t) buffer[3] << 32)
           | ((uint64_t) buffer[4] << 24)
           | ((uint64_t) buffer[5] << 16)
           | ((uint64_t) buffer[6] << 8)
           | (uint64_t) buffer[7];

}

void little_endian_write_uint16(uint8_t *buffer, uint16_t n) {
    buffer[1] = (uint8_t) (n >> 8);
    buffer[0] = (uint8_t) n;
}

void little_endian_write_uint32(uint8_t *buffer, uint32_t n) {
    buffer[3] = (uint8_t) (n >> 24);
    buffer[2] = (uint8_t) (n >> 16);
    buffer[1] = (uint8_t) (n >> 8);
    buffer[0] = (uint8_t) n;
}

void little_endian_write_uint64(uint8_t *buffer, uint64_t n) {
    buffer[7] = (uint8_t) (n >> 56);
    buffer[6] = (uint8_t) (n >> 48);
    buffer[5] = (uint8_t) (n >> 40);
    buffer[4] = (uint8_t) (n >> 32);
    buffer[3] = (uint8_t) (n >> 24);
    buffer[2] = (uint8_t) (n >> 16);
    buffer[1] = (uint8_t) (n >> 8);
    buffer[0] = (uint8_t) n;
}

uint16_t little_endian_read_uint16(const uint8_t *buffer) {
    return ((uint16_t) buffer[1] << 8) | (uint16_t) buffer[0];
}

uint32_t little_endian_read_uint32(const uint8_t *buffer) {
    return ((uint32_t) buffer[3] << 24)
           | ((uint32_t) buffer[2] << 16)
           | ((uint32_t) buffer[1] << 8)
           | (uint32_t) buffer[0];
}

uint64_t little_endian_read_uint64(const uint8_t *buffer) {
    return ((uint64_t) buffer[7] << 56)
           | ((uint64_t) buffer[6] << 48)
           | ((uint64_t) buffer[5] << 40)
           | ((uint64_t) buffer[4] << 32)
           | ((uint64_t) buffer[3] << 24)
           | ((uint64_t) buffer[2] << 16)
           | ((uint64_t) buffer[1] << 8)
           | (uint64_t) buffer[0];
}

