helloJavaWorld <- function() {
  hjw <- .jnew("HelloWorld") # create instance of Hello World class
  out <- .jcall(hjw, "S", "sayHello") #invoke sayHello method
  return (out)
}

