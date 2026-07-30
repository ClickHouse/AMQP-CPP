/**
 *  TcpInBuffer.h
 *
 *  Implementation of byte byte-buffer used for incoming frames
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2016 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
 #include <openssl/ssl.h>
 #include <algorithm>

/**
 *  Beginnig of namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class TcpInBuffer : public ByteBuffer
{
private:
    /**
     *  Number of bytes that are allocated for the buffer (the _size member of the base
     *  class only tells how many of them are filled with data)
     *  @var size_t
     */
    size_t _capacity;

public:
    /**
     *  Constructor
     *  Note that we pass 0 to the constructor because the buffer seems to be empty
     *  @param  size        initial size to allocated
     */
    TcpInBuffer(size_t size)
        // if malloc failed, `_data` is null and the buffer has no room; reflecting that in
        // `_capacity` keeps room()/receivefrom from treating a null buffer as writable
        : ByteBuffer((char *)malloc(size), 0), _capacity(_data ? size : 0) {}

    /**
     *  No copy'ing
     *  @param  that        object to copy
     */
    TcpInBuffer(const TcpInBuffer &that) = delete;

    /**
     *  Move constructor
     *  @param  that
     */
    TcpInBuffer(TcpInBuffer &&that) : ByteBuffer(std::move(that)), _capacity(that._capacity)
    {
        // the other object no longer owns memory
        that._capacity = 0;
    }

    /**
     *  Destructor
     */
    virtual ~TcpInBuffer()
    {
        // free memory
        if (_data) free((void *)_data);
    }

    /**
     *  Move assignment operator
     *  @param  that
     */
    TcpInBuffer &operator=(TcpInBuffer &&that)
    {
        // skip self-assignment
        if (this == &that) return *this;
        
        // call base
        ByteBuffer::operator=(std::move(that));

        // take over the allocated size too
        _capacity = that._capacity;
        that._capacity = 0;

        // done
        return *this;
    }

    /**
     *  Reallocate date
     *  @param  size
     */
    void reallocate(size_t size)
    {
        // update data
        auto *data = (char *)realloc((void *)_data, size);

        // leave the old buffer in place when the allocation failed, so that we do not
        // end up with a null buffer that is still considered to have capacity
        if (data == nullptr) return;

        // remember the new buffer and how much room it has
        _data = data;
        _capacity = size;
   }

    /**
     *  Number of bytes that are still free in the buffer
     *  @return size_t
     */
    size_t room() const
    {
        return _capacity > _size ? _capacity - _size : 0;
    }

    /**
     *  Number of bytes that we still need to complete the frame that is being received
     *  @param  expected        total number of bytes that the library expects
     *  @return uint32_t
     */
    uint32_t wanted(uint32_t expected) const
    {
        return expected > _size ? expected - (uint32_t)_size : 0;
    }

    /**
     *  Receive data from a socket
     *  @param  socket          socket to read from
     *  @param  expected        number of bytes that the library expects
     *  @return ssize_t
     */
    ssize_t receivefrom(int socket, uint32_t expected)
    {
        // find out how many bytes are available
        uint32_t available = 0;
        
        // check the number of bytes that are available
        if (ioctl(socket, FIONREAD, &available) != 0) return -1;
        
        // if no bytes are available, it could mean that the connection was closed
        // by the remote client, so we do have to call read() anyway, assume a default buffer
        if (available == 0) available = 1;

        // number of bytes to read, never more than what is still free in the buffer
        size_t bytes = std::min({ (size_t)wanted(expected), (size_t)available, room() });

        // read data into the buffer
        auto result = read(socket, (void *)(_data + _size), bytes);
        
        // update total buffer size
        if (result > 0) _size += result;
        
        // done
        return result;
    }

    /**
     *  Receive data from a socket
     *  @param  ssl             ssl wrapped socket to read from
     *  @param  expected        number of bytes that the library expects
     *  @return ssize_t
     */
    ssize_t receivefrom(SSL *ssl, uint32_t expected)
    {
        // number of bytes to that still fit in the buffer
        size_t bytes = std::min((size_t)wanted(expected), room());

        // read data
        auto result = OpenSSL::SSL_read(ssl, (void *)(_data + _size), bytes);
        
        // update total buffer size on success
        if (result > 0) _size += result;
        
        // done
        return result;
    }
    
    /**
     *  Shrink the buffer (in practice this is always called with the full buffer size)
     *  @param  size
     */
    void shrink(size_t size)
    {
        // update size
        _size -= size;
    }
};

/**
 *  End of namespace
 */
}

