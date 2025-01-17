/**
 *  OpenSSL.cpp
 *
 *  Implementation file for the openssl.h header file
 *
 *  @copyright 2018 Copernica BV
 */

/**
 *  Dependencies
 */
#include "openssl.h"
#include "function.h"
#include "amqpcpp/openssl.h"

/**
 *  The handle to access openssl (the result of dlopen("openssl.so"))
 *  By default we set this to RTLD_DEFAULT, so that AMQP-CPP checks the internal process space
 */
static void *handle = RTLD_DEFAULT;

/**
 *  Just the AMQP namespace
 */
namespace AMQP {

/**
 *  Function to set the openssl handle
 *  @param  ptr
 */
void openssl(void *ptr)
{
    // store handle
    handle = ptr;
}

/**
 *  Begin of AMQP::OpenSSL namespace
 */
namespace OpenSSL {

/**
 *  Is the openssl library loaded?
 *  @return bool
 */
bool valid()
{
    if (handle == RTLD_DEFAULT)
        return true;

    // create a function
    static Function<decltype(::SSL_CTX_new)> func(handle, "SSL_CTX_new");

    // we need a library
    return func;
}

/**
 *  Get the SSL_METHOD for outgoing connections
 *  @return SSL_METHOD *
 */
const SSL_METHOD *TLS_client_method()
{
    if (handle == RTLD_DEFAULT)
        return ::TLS_client_method();

    // create a function that loads the method
    static Function<decltype(TLS_client_method)> func(handle, "TLS_client_method");

    // call the openssl function
    if (func) return func();

    // older openssl libraries do not have this function, so we try to load an other function
    static Function<decltype(TLS_client_method)> old(handle, "SSLv23_client_method");

    // call the old one
    return old();
}

/**
 *  Create new SSL context
 *  @param  method  SSL_METHOD can be of the following types: TLS_method(), TLS_server_method(), TLS_client_method()
 *  @return         pointer to object
 */
SSL_CTX *SSL_CTX_new(const SSL_METHOD *method)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_CTX_new(method);

    // create a function
    static Function<decltype(::SSL_CTX_new)> func(handle, "SSL_CTX_new");

    // call the openssl function
    return func(method);
}

/**
 *  Read data from an ssl socket
 *  @param  ssl     ssl structure
 *  @param  buf     buffer to read into
 *  @param  num     size of buffer
 *  @return int     number of bytes read
 */
int SSL_read(SSL *ssl, void *buf, int num)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_read(ssl, buf, num);

    // create a function
    static Function<decltype(::SSL_read)> func(handle, "SSL_read");

    // call the openssl function
    return func(ssl, buf, num);
}

/**
 *  Write data to an ssl socket
 *  @param  ssl     ssl structure
 *  @param  buf     buffer to write
 *  @param  num     size of buffer
 *  @return int     number of bytes written
 */
int SSL_write(SSL *ssl, const void *buf, int num)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_write(ssl, buf, num);

    // create a function
    static Function<decltype(::SSL_write)> func(handle, "SSL_write");

    // call the openssl function
    return func(ssl, buf, num);
}

/**
 *  Connect the SSL object with a file descriptor
 *  @param  ssl     SSL object
 *  @param  fd      file descriptor
 *  @return int     wether the operation succeeded or not
 */
int SSL_set_fd(SSL *ssl, int fd)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_set_fd(ssl, fd);

    // create a function
    static Function<decltype(::SSL_set_fd)> func(handle, "SSL_set_fd");

    // call the openssl function
    return func(ssl, fd);
}

/**
 *  The number of bytes availabe in the ssl struct that have been read
 *  from the socket, but that have not been returned the SSL_read()
 *  @param  ssl     SSL object
 *  @return int     number of bytes
 */
int SSL_pending(const SSL *ssl)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_pending(ssl);

    // create a function
    static Function<decltype(::SSL_pending)> func(handle, "SSL_pending");

    // call the openssl function
    return func(ssl);
}

/**
 *  Free an allocated ssl context
 *  @param  ctx
 */
void SSL_CTX_free(SSL_CTX *ctx)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_CTX_free(ctx);

    // create a function
    static Function<decltype(::SSL_CTX_free)> func(handle, "SSL_CTX_free");

    // call the openssl function
    return func(ctx);
}

/**
 *  Free an allocated SSL structure
 *  @param  ssl     SSL object to be freed
 *  @return int     wether the operation succeeded or not
 */
void SSL_free(SSL *ssl)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_free(ssl);

    // create a function
    static Function<decltype(::SSL_free)> func(handle, "SSL_free");

    // call the openssl function
    return func(ssl);
}

/**
 *  Create a new SSL structure for a connection
 *  @param  ctx     SSL context object
 *  @return SSL     the created SSL oject based on th context
 */
SSL *SSL_new(SSL_CTX *ctx)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_new(ctx);

    // create a function
    static Function<decltype(::SSL_new)> func(handle, "SSL_new");

    // call the openssl function
    return func(ctx);
}

/**
 *  Increment refcount for a ssl structure
 *  @param  ctx     SSL structure
 *  @return int     1 for success, 0 for failure
 */
int SSL_up_ref(SSL *ssl)
{
    // create a function
    static Function<decltype(SSL_up_ref)> func(handle, "SSL_up_ref");

    // call the openssl function if it exists
    if (func) return func(ssl);

    // @todo use our own implementation

    return 0;
}

/**
 *  Shut down a TLS/SSL shut down
 *  @param  ssl     SSL object to terminate
 *  @return int     returns diagnostic values
 */
int SSL_shutdown(SSL *ssl)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_shutdown(ssl);

    // create a function
    static Function<decltype(::SSL_shutdown)> func(handle, "SSL_shutdown");

    // call the openssl function
    return func(ssl);
}

/**
 *  Prepare SSL object to work in client or server mode
 *  @param  ssl     SSL object to set connect state on
 */
void SSL_set_connect_state(SSL *ssl)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_set_connect_state(ssl);

    // create a function
    static Function<decltype(::SSL_set_connect_state)> func(handle, "SSL_set_connect_state");

    // call the openssl function
    func(ssl);

}

/**
 *  Perform a TLS/SSL handshake
 *  @param  ssl     SSL object
 *  @return int     returns diagnostic values
 */
int SSL_do_handshake(SSL *ssl)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_do_handshake(ssl);

    // create a function
    static Function<decltype(::SSL_do_handshake)> func(handle, "SSL_do_handshake");

    // call the openssl function
    return func(ssl);
}

/**
 *  Obtain shutdown statue for TLS/SSL I/O operation
 *  @param  ssl     SSL object
 *  @return int     returns error values
 */
int SSL_get_shutdown(const SSL *ssl)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_get_shutdown(ssl);

    // create a function
    static Function<decltype(::SSL_get_shutdown)> func(handle, "SSL_get_shutdown");

    // call the openssl function
    return func(ssl);
}

/**
 *  Obtain result code for TLS/SSL I/O operation
 *  @param  ssl     SSL object
 *  @param  ret     the returned diagnostic value of SSL calls
 *  @return int     returns error values
 */
int SSL_get_error(const SSL *ssl, int ret)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_get_error(ssl, ret);

    // create a function
    static Function<decltype(::SSL_get_error)> func(handle, "SSL_get_error");

    // call the openssl function
    return func(ssl, ret);
}

/**
 *  Set the certificate file to be used by the connection
 *  @param  ssl     ssl structure
 *  @param  file    filename
 *  @param  type    type of file
 *  @return int
 */
int SSL_use_certificate_file(SSL *ssl, const char *file, int type)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_use_certificate_file(ssl, file, type);

    // create a function
    static Function<decltype(::SSL_use_certificate_file)> func(handle, "SSL_use_certificate_file");

    // call the openssl function
    return func(ssl, file, type);
}

/**
 *  Specify that the default location from which CA certificates are loaded
 *  should be used.
 *  @param  ctx
 */
int SSL_CTX_set_default_verify_paths(SSL_CTX *ctx)
{
    if (handle == RTLD_DEFAULT)
        return ::SSL_CTX_set_default_verify_paths(ctx);

    // the actual function
    static Function<decltype(::SSL_CTX_set_default_verify_paths)> func(handle, "SSL_CTX_set_default_verify_paths");

    // call actual function
    return func(ctx);
}

/**
 *  Clear the SSL error queue
 *  @return void
 */
void ERR_clear_error()
{
    if (handle == RTLD_DEFAULT)
        return ::ERR_clear_error();

    // create a function
    static Function<decltype(::ERR_clear_error)> func(handle, "ERR_clear_error");

    // call the openssl function
    return func();
}


/// SSL_set_tlsext_host_name is a function in BoringSSL but macro in OpenSSL. Wrap into function.
static int SSL_set_tlsext_host_name_impl(SSL *ssl, const char *name)
{
    return SSL_set_tlsext_host_name(ssl, name);
}

int SSL_set_tlsext_host_name_func(SSL *ssl, const char *name)
{
    if (handle == RTLD_DEFAULT)
        return SSL_set_tlsext_host_name_impl(ssl, name);

    static Function<decltype(SSL_set_tlsext_host_name_impl)> func(handle, "SSL_set_tlsext_host_name");
    return func(ssl, name);
}

static uint32_t SSL_CTX_set_mode_impl(SSL_CTX *ctx, uint32_t mode)
{
    return SSL_CTX_set_mode(ctx, mode);
}

uint32_t SSL_CTX_set_mode_func(SSL_CTX *ctx, uint32_t mode)
{
    if (handle == RTLD_DEFAULT)
        return SSL_CTX_set_mode_impl(ctx, mode);

    static Function<decltype(SSL_CTX_set_mode_impl)> func(handle, "SSL_CTX_set_mode");
    return func(ctx, mode);
}


/**
 *  Print errors via a callback
 *  @param  cb
 *  @param  u
 */
void ERR_print_errors_cb(int (*cb)(const char *str, size_t len, void *u), void *u)
{
    if (handle == RTLD_DEFAULT)
    {
        ::ERR_print_errors_cb(cb, u);
        return;
    }

    // the actual function
    static Function<decltype(::ERR_print_errors_cb)> func(handle, "ERR_print_errors_cb");

    // call the openssl function
    func(cb, u);
}

/**
 *  End of namespace
 */
}}

