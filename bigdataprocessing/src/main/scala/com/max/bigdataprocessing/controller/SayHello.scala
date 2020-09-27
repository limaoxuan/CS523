package com.max.bigdataprocessing.controller

import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RestController}

@RestController
class SayHello {


  @RequestMapping(value = Array("/test"), method = Array(RequestMethod.GET))
  def sayHello() = {
    "hello"
  }


}
