const { ApolloServer, gql } = require('apollo-server')
const amqp = require('amqplib')

async function MQClient() {

  const connection = await amqp.connect('amqp://localhost')

  const typeDefs = gql`
    type Query {
      text: String
    }
  `

  const resolvers = {
    Query: {
      text: async () => await send(connection)
    },
  }

  const server = new ApolloServer({ typeDefs, resolvers })

  server.listen().then(({ url }) => {
    console.log(`Server ready at ${url}`)
  })
}

function send(connection) {
  return new Promise((resolve, reject) => {

    const keyRoute = "server.resolve"
    const exchange = "exchange"
    const dynamicKey = "graphql.client." + (Math.random() * (10000 - 10) + 10).toString()

    connection.createChannel()
      .then((channel) => {
        channel.assertExchange(exchange, 'topic', { durable: false })
          .then(() => {
            channel.assertQueue('', { durable: true })
              .then(({ queue }) => {
                channel.bindQueue(queue, exchange, dynamicKey)
                  .then(() => {
                    channel.publish(exchange, keyRoute, Buffer.from('Tarea en proceso'), { persistent: true, replyTo: dynamicKey })

                    let consumerTag = ""
                    channel.consume(queue, (msg) => {

                      console.log(msg.content.toString());
                      channel.ack(msg)

                      resolve(msg.content.toString())

                      channel.cancel(consumerTag).then(() => {
                        return channel.unbindQueue(queue, exchange, dynamicKey)
                      })
                        .then(() => {
                          return channel.deleteQueue(queue)
                        })
                        .then(() => {
                          console.log('Todo cerrado');
                          return channel.close()
                        })

                    }, {
                      noAck: false
                    }).then(({ consumerTag: valueTag }) => {
                      consumerTag = valueTag
                    })
                  })
              })
          })
      })
  })
}


MQClient()