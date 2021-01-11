var amqp = require('amqplib')

const queue = 'task_queue'
const exchange = "exchange"
const thisKey = "server.resolve"

async function ServerMQ() {

    const connection = await amqp.connect('amqp://localhost')
    const channel = await connection.createChannel()


    await channel.assertExchange(exchange, 'topic', { durable: false })
    await channel.assertQueue(queue, { durable: true })
    await channel.bindQueue(queue, exchange, thisKey)
    await channel.prefetch(1)

    channel.consume(queue, (msg) => {
        console.log("[RECEIVE]")
        channel.ack(msg)
        between(channel, msg).then((res)=>{
            console.log(res);
        })
    }, {
        noAck: false
    })

}

function between(channel, msg) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {

            let variable = Math.floor(
                Math.random() * (1000 - 10) + 10
            )

            channel.publish(exchange, msg.properties.replyTo, Buffer.from(variable.toString()), { persistent: true })
            resolve(variable)

        }, 10000)
    })
}

ServerMQ()