import pika

import sys
import time
import os
import multiprocessing
import ctypes

def create_test_msg(msg_size):
    class TEST(ctypes.Structure):
        _fields_ = [('data', ctypes.c_byte * msg_size)]
    return TEST

def publisher_loop(pub_id=0, num_msgs=10000, msg_size=512, num_subscribers=1, ready_flag=None, server='localhost'):
    # Setup Client
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='TEST', exchange_type='direct')

    channel.exchange_declare(exchange='SUBSCRIBER_READY', exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True)
    sub_ready_queue_name = result.method.queue
    
    channel.queue_bind(
        exchange='SUBSCRIBER_READY', queue=sub_ready_queue_name)

    # Signal that publisher is ready
    if ready_flag is not None:
        ready_flag.set()

    # Wait for the subscribers to be ready
    num_subscribers_ready = 0

    def sub_ready_callback(channel, method, properties, body):
        nonlocal num_subscribers_ready
        num_subscribers_ready += 1
        if num_subscribers_ready >= num_subscribers:
            channel.stop_consuming() # subscribers are ready

    if num_subscribers_ready < num_subscribers:
        channel.basic_consume(
            queue=sub_ready_queue_name, on_message_callback=sub_ready_callback, auto_ack=True)
        channel.start_consuming() # wait for subscribers to be ready

    # Create TEST message with dummy data
    data = create_test_msg(msg_size)()
    data.data[:] = list(range(msg_size))

    # Send loop
    tic = time.perf_counter()
    for n in range(num_msgs):
        channel.basic_publish(exchange='TEST', routing_key='TEST_DATA', body=bytes(data.data))
    toc = time.perf_counter()

     # Stats
    test_msg_size = ctypes.sizeof(data) #HEADER_SIZE + ctypes.sizeof(data)
    dur = (toc-tic)
    data_rate = test_msg_size * num_msgs / float(1048576) / dur
    print(f"Publisher[{pub_id}] -> {num_msgs} messages | {int((num_msgs)/dur)} messages/sec | {data_rate:0.1f} MB/sec | {dur:0.6f} sec ")    
    
    connection.close()

def subscriber_loop(sub_id, num_msgs, msg_size=512, server='localhost'):
    # Setup Client
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    
    channel.exchange_declare(exchange='SUBSCRIBER_READY', exchange_type='fanout')
    channel.exchange_declare(exchange='TEST', exchange_type='direct')
    channel.exchange_declare(exchange='EXIT', exchange_type='fanout')
    
    result = channel.queue_declare(queue='', exclusive=True)
    test_data_queue_name = result.method.queue
    result = channel.queue_declare(queue='', exclusive=True)
    exit_queue_name = result.method.queue

    channel.queue_bind(
        exchange='TEST', queue=test_data_queue_name, routing_key='TEST_DATA')
    channel.queue_bind(
        exchange='EXIT', queue=exit_queue_name)

    # Send Subscriber Ready
    channel.basic_publish(exchange='SUBSCRIBER_READY', routing_key='', body='')

    # Read Loop (Start clock after first TEST msg received)
    abort_timeout = max(num_msgs/10000, 10) #seconds
    abort_start = time.perf_counter()

    msg_count = 0
    tic = time.perf_counter()
    toc = time.perf_counter()

    def exit_callback(channel, method, properties, body):
        print('Got EXIT')
        channel.stop_consuming() # subscribers are ready
    def data_rcv_callback(channel, method, properties, body):
        nonlocal msg_count
        nonlocal tic
        nonlocal toc
        if msg_count == 0:
            tic = time.perf_counter()
        toc = time.perf_counter()
        msg_count += 1
        if msg_count >= num_msgs:
            channel.stop_consuming() # all messages received

        if time.perf_counter() - abort_start > abort_timeout: 
            print(f"Subscriber [{sub_id:d}] Timed out.")
            channel.stop_consuming()


    channel.basic_consume(
        queue=test_data_queue_name, on_message_callback=data_rcv_callback, auto_ack=True)
    channel.basic_consume(
        queue=exit_queue_name, on_message_callback=exit_callback, auto_ack=True)
    channel.start_consuming() # read messages

    # Stats
    msg_data = create_test_msg(msg_size)()
    test_msg_size = ctypes.sizeof(msg_data)
    dur = toc - tic
    data_rate = (test_msg_size * num_msgs) / float(1048576) / dur
    if msg_count == num_msgs:
        print(f"Subscriber [{sub_id:d}] -> {msg_count} messages | {int((msg_count-1)/dur)} messages/sec | {data_rate:0.1f} MB/sec | {dur:0.6f} sec ")
    else:
        print(f"Subscriber [{sub_id:d}] -> {msg_count} ({int(msg_count/num_msgs *100):0d}%) messages | {int((msg_count-1)/dur)} messages/sec | {data_rate:0.1f} MB/sec | {dur:0.6f} sec ")

    connection.close()


if __name__ == '__main__':
    import argparse

    # Configuration flags for bench utility
    parser = argparse.ArgumentParser(description='RabbittMQ Client bench test utility')
    parser.add_argument('-ms', default=128, type=int, dest='msg_size', help='Messge size in bytes.')
    parser.add_argument('-n', default=10000, type=int, dest='num_msgs', help='Number of messages.')
    parser.add_argument('-np', default=1, type=int, dest='num_publishers', help='Number of concurrent publishers.')
    parser.add_argument('-ns', default=1, type=int, dest='num_subscribers', help='Number of concurrent subscribers.')
    parser.add_argument('-s',default='localhost', dest='server', help='RabbitMQ message broker ip address (default: ''localhost'')')
    args = parser.parse_args()

    # Main Thread client
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='EXIT', exchange_type='fanout')
    #channel.queue_declare(queue='', exclusive=True)

    print("Initializing producer processses...")
    publisher_ready = []
    publishers = []
    for n in range(args.num_publishers):
        publisher_ready.append(multiprocessing.Event())
        publishers.append(
                multiprocessing.Process(
                    target=publisher_loop,
                    kwargs={
                        'pub_id': n+1,
                        'num_msgs': int(args.num_msgs/args.num_publishers),
                        'msg_size': args.msg_size, 
                        'num_subscribers': args.num_subscribers,
                        'ready_flag': publisher_ready[n],
                        'server': args.server})
                    )
        publishers[n].start()

    # Wait for publisher processes to be established
    for flag in publisher_ready:
        flag.wait()

    print('Waiting for subscriber processes...')
    subscribers = []
    for n in range(args.num_subscribers):
        subscribers.append(
                multiprocessing.Process(
                    target=subscriber_loop,
                    args=(n+1, args.num_msgs, args.msg_size),
                    kwargs={'server':args.server}))
        subscribers[n].start()

    print("Starting Test...")
    #print(f"RabbitMQ packet size: {HEADER_SIZE + args.msg_size}")
    print(f'Sending {args.num_msgs} messages...')

    # Wait for publishers to finish
    for publisher in publishers:
        publisher.join()

    # Wait for subscribers to finish
    abort_timeout = max(args.num_msgs/10000, 10) #seconds
    abort_start = time.perf_counter()
    abort = False

    while not abort:
        subscribers_finished = 0
        for subscriber in subscribers:
            if subscriber.exitcode is not None:
                subscribers_finished += 1

        if subscribers_finished == len(subscribers):
            break

        if time.perf_counter() - abort_start > abort_timeout: 
            channel.basic_publish(exchange='EXIT', routing_key='', body='')
            print('Test Timeout! Sending Exit Signal...')
            abort = True

    for subscriber in subscribers:
        subscriber.join()
    
    connection.close()

    print('Done!')