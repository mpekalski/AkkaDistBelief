package com.github.alexminnaar.AkkaDistBelief.actors

/**
  * Created by marcin on 2016-11-26.
  */
  import akka.actor.Actor
  import akka.actor.ActorSystem
  import akka.actor.Props

  class HelloActor extends Actor {
    def receive = {
      case "hello" => println("hello back at you")
      case _       => println("huh?")
    }
  }

  object Main2 extends App {
    val system = ActorSystem("HelloSystem")
    // default Actor constructor
    val helloActor = system.actorOf(Props[HelloActor], name = "helloactor")
    helloActor ! "hello"
    helloActor ! "buenos dias"
  }
