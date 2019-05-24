//
// Created by zhachen on 2019-05-13.
//

#include "test_msg.breeze.h"

int TestSubMsg::write_to(BytesBuffer *buf) const {
    return breeze::write_message(buf, "TestSubMsg", [this, buf]() {
        breeze::write_message_field(buf, 1, this->s);
        breeze::write_message_field(buf, 2, this->i32);
        breeze::write_message_field(buf, 3, this->i64);
        breeze::write_message_field(buf, 4, this->f);
        breeze::write_message_field(buf, 5, this->d);
        breeze::write_message_field(buf, 6, this->byte);
        breeze::write_message_field(buf, 7, this->bytes);
        breeze::write_message_field(buf, 8, this->map1);
        breeze::write_message_field(buf, 9, this->map2);
        breeze::write_message_field(buf, 10, this->list);
        breeze::write_message_field(buf, 11, this->b);
    });
}

int TestSubMsg::read_from(BytesBuffer *buf) {
    return breeze::read_message_by_field(buf, [this](BytesBuffer *buf, int index) {
        switch (index) {
            case 1:
                return breeze::read_value(buf, this->s);
            case 2:
                return breeze::read_value(buf, this->i32);
            case 3:
                return breeze::read_value(buf, this->i64);
            case 4:
                return breeze::read_value(buf, this->f);
            case 5:
                return breeze::read_value(buf, this->d);
            case 6:
                return breeze::read_value(buf, this->byte);
            case 7:
                return breeze::read_value(buf, this->bytes);
            case 8:
                return breeze::read_value(buf, this->map1);
            case 9:
                return breeze::read_value(buf, this->map2);
            case 10:
                return breeze::read_value(buf, this->list);
            case 11:
                return breeze::read_value(buf, this->b);
            default:
                return -1;
        }
    });
}

std::string TestSubMsg::get_name() const {
    return "TestSubMsg";
}

std::string TestSubMsg::get_alias() {
    return "TestSubMsg";
}

std::shared_ptr<BreezeSchema> TestSubMsg::get_schema() {
    return std::shared_ptr<BreezeSchema>();
}

void TestSubMsg::set_name(const std::string &name) {}

int TestMsg::write_to(BytesBuffer *buf) const {
    return breeze::write_message(buf, "TestMsg", [this, buf]() {
        breeze::write_message_field(buf, 1, this->i);
        breeze::write_message_field(buf, 2, this->s);
        breeze::write_message_field(buf, 3, this->m);
        breeze::write_message_field(buf, 4, this->a);
    });
}

int TestMsg::read_from(BytesBuffer *buf) {
    return breeze::read_message_by_field(buf, [this](BytesBuffer *buf, int index) {
        switch (index) {
            case 1:
                return breeze::read_value(buf, this->i);
            case 2:
                return breeze::read_value(buf, this->s);
            case 3:
                return breeze::read_value(buf, this->m);
            case 4:
                return breeze::read_value(buf, this->a);
            default:
                return -1;
        }
    });
}

std::string TestMsg::get_name() const {
    return "TestMsg";
}

std::string TestMsg::get_alias() {
    return "TestMsg";
}

std::shared_ptr<BreezeSchema> TestMsg::get_schema() {
    return std::shared_ptr<BreezeSchema>{};
}

void TestMsg::set_name(const std::string &name) {}
