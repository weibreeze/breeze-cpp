//
// Created by zhachen on 2019-05-23.
//

#ifndef BREEZE_CPP_BUFFER_H
#define BREEZE_CPP_BUFFER_H

#include <cstdio>
#include <cstdint>
#include <cstdarg>
#include <cstdlib>
#include <cassert>
#include <cstring>
#include <iostream>

#define MOTAN_OK 0
#define E_MOTAN_BUFFER_NOT_ENOUGH (-1)
#define E_MOTAN_OVERFLOW (-2)
#define E_MOTAN_UNSUPPORTED_TYPE (-3)
#define E_MOTAN_MEMORY_NOT_ENOUGH (-4)
#define E_MOTAN_WRONG_SIZE (-5)
#define E_MOTAN_MISMATCH_TYPE (-6)

enum ByteOrder {
    kBigEndian,
    kLittleEndian
};

class BytesBuffer {
public:
    uint8_t *buffer_;
    ByteOrder order_;
    uint32_t write_pos_;
    uint32_t read_pos_;
    size_t capacity_;
    uint8_t read_only_;

    void dump();

    void free_bytes_buffer();

    void grow_buffer(const size_t &n);

    void write_bytes(const uint8_t *bytes, const int &len);

    void write_byte(const uint8_t &u);

    void write_uint16(const uint16_t &u);

    void write_uint32(const uint32_t &u);

    void write_uint64(const uint64_t &u);

    int write_varint(uint64_t u);

    void set_write_pos(const uint32_t &pos);

    void set_read_pos(const uint32_t &pos);

    int length();

    int remain();

    int next(const int &n, BytesBuffer &ret);

    void reset();

    int read_bytes(uint8_t *bs, const int &len);

    int read_byte(uint8_t &u);

    int read_uint16(uint16_t &u);

    int read_uint32(uint32_t &u);

    int read_uint64(uint64_t &u);

    int read_varint(uint64_t &u);

    int write_zigzag32(const uint32_t &v);

    int write_zigzag64(const uint64_t &v);

    int read_zigzag32(uint64_t &v);

    int read_zigzag64(uint64_t &v);
};

void die_bytes_buffer(const char *fmt, ...);

void dump_bytes_buffer(const uint8_t *bs, const int &len);

BytesBuffer *new_bytes_buffer(const size_t &capacity, const ByteOrder &order = kBigEndian);

BytesBuffer *new_bytes_buffer_from_bytes(const uint8_t *raw_bytes, const size_t &len,
                                         const ByteOrder &order = kBigEndian, const uint8_t &read_only = 0);

uint64_t zigzag_encode(int64_t u);

int64_t zigzag_decode(uint64_t n);

// 高位低址
void big_endian_write_uint16(uint8_t *buffer, uint16_t n);

void big_endian_write_uint32(uint8_t *buffer, uint32_t n);

void big_endian_write_uint64(uint8_t *buffer, uint64_t n);

uint16_t big_endian_read_uint16(const uint8_t *buffer);

uint32_t big_endian_read_uint32(const uint8_t *buffer);

uint64_t big_endian_read_uint64(const uint8_t *buffer);

// 高位高址
void little_endian_write_uint16(uint8_t *buffer, uint16_t n);

void little_endian_write_uint32(uint8_t *buffer, uint32_t n);

void little_endian_write_uint64(uint8_t *buffer, uint64_t n);

uint16_t little_endian_read_uint16(const uint8_t *buffer);

uint32_t little_endian_read_uint32(const uint8_t *buffer);

uint64_t little_endian_read_uint64(const uint8_t *buffer);

#endif //BREEZE_CPP_BUFFER_H
