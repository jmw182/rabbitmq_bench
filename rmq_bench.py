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

def publisher_loop(pub_id=0, num_msgs=10000, msg_size=512, num_subscribers=1, server='localhost'):
    # Setup Client
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='TEST', exchange_type='direct')

    channel.exchange_declare(exchange='THREAD_STATUS', exchange_type='direct')
    result = channel.queue_declare(queue='', exclusive=True)
    sub_ready_queue_name = result.method.queue
    
    channel.queue_bind(
        exchange='THREAD_STATUS', queue=sub_ready_queue_name, routing_key='SUBSCRIBER_READY')

    # Signal that publisher is ready
    channel.basic_publish(exchange='THREAD_STATUS', routing_key='PUBLISHER_READY', body='')

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

    channel.basic_publish(exchange='THREAD_STATUS', routing_key='PUBLISHER_DONE', body='')

     # Stats
    test_msg_size = ctypes.sizeof(data) #HEADER_SIZE + ctypes.sizeof(data)
    dur = (toc-tic)
    data_rate = test_msg_size * num_msgs / 1e6 / dur
    print(f"Publisher[{pub_id}] -> {num_msgs} messages | {int((num_msgs)/dur)} messages/sec | {data_rate:0.1f} MB/sec | {dur:0.6f} sec ")    
    
    connection.close()

def subscriber_loop(sub_id, num_msgs, msg_size=512, server='localhost'):
    # Setup Client
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    
    channel.exchange_declare(exchange='THREAD_STATUS', exchange_type='direct')
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
    channel.basic_publish(exchange='THREAD_STATUS', routing_key='SUBSCRIBER_READY', body='')

    # Read Loop (Start clock after first TEST msg received)
    msg_count = 0
    tic = time.perf_counter()
    toc = time.perf_counter()

    def exit_callback(channel, method, properties, body):
        print('Got EXIT')
        channel.stop_consuming() # test timeout
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

    channel.basic_consume(
        queue=test_data_queue_name, on_message_callback=data_rcv_callback, auto_ack=True)
    channel.basic_consume(
        queue=exit_queue_name, on_message_callback=exit_callback, auto_ack=True)
    channel.start_consuming() # read messages

    channel.basic_publish(exchange='THREAD_STATUS', routing_key='SUBSCRIBER_DONE', body='')

    # Stats
    msg_data = create_test_msg(msg_size)()
    test_msg_size = ctypes.sizeof(msg_data)
    dur = toc - tic
    data_rate = (test_msg_size * num_msgs) / 1e6 / dur
    if msg_count == num_msgs:
        print(f"Subscriber [{sub_id:d}] -> {msg_count} messages | {int((msg_count-1)/dur)} messages/sec | {data_rate:0.1f} MB/sec | {dur:0.6f} sec ")
    else:
        print(f"Subscriber [{sub_id:d}] -> {msg_count} ({int(msg_count/num_msgs *100):0d}%) messages | {int((msg_count-1)/dur)} messages/sec | {data_rate:0.1f} MB/sec | {dur:0.6f} sec ")

    connection.close()

def heartbeat_loop(server='localhost'):
    # Setup Client
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='HEARTBEAT', exchange_type='fanout')
    channel.exchange_declare(exchange='EXIT', exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True)
    exit_queue_name = result.method.queue
    channel.queue_bind(
        exchange='EXIT', queue=exit_queue_name)
   
    # Send loop
    exit = False
    while not exit:
        method, properties, body = channel.basic_get(exit_queue_name, auto_ack = True)  
        if method is not None:
            print('Heartbeat thread got EXIT')
            exit = True
            break
        channel.basic_publish(exchange='HEARTBEAT', routing_key='', body='')
        time.sleep(1)
    
    connection.close()

def main(args):
    # Main Thread client
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='EXIT', exchange_type='fanout')
    channel.exchange_declare(exchange='THREAD_STATUS', exchange_type='direct')
    channel.exchange_declare(exchange='HEARTBEAT', exchange_type='fanout')


    result = channel.queue_declare(queue='', exclusive=True)
    sub_ready_queue_name = result.method.queue
    channel.queue_bind(
        exchange='THREAD_STATUS', queue=sub_ready_queue_name, routing_key='SUBSCRIBER_READY')

    result = channel.queue_declare(queue='', exclusive=True)
    pub_ready_queue_name = result.method.queue
    channel.queue_bind(
        exchange='THREAD_STATUS', queue=pub_ready_queue_name, routing_key='PUBLISHER_READY')    

    result = channel.queue_declare(queue='', exclusive=True)
    sub_done_queue_name = result.method.queue
    channel.queue_bind(
        exchange='THREAD_STATUS', queue=sub_done_queue_name, routing_key='SUBSCRIBER_DONE')    

    result = channel.queue_declare(queue='', exclusive=True)
    pub_done_queue_name = result.method.queue
    channel.queue_bind(
        exchange='THREAD_STATUS', queue=pub_done_queue_name, routing_key='PUBLISHER_DONE')

    result = channel.queue_declare(queue='', exclusive=True)
    heartbeat_queue_name = result.method.queue
    channel.queue_bind(
        exchange='HEARTBEAT', queue=heartbeat_queue_name)

    sys.stdout.write(f"Packet size: {args.msg_size} bytes\n")
    sys.stdout.write(f'Sending {args.num_msgs} messages...\n')
    sys.stdout.flush()

    # initialize heartbeat thread
    heartbeat_thread = multiprocessing.Process(
                        target=heartbeat_loop,
                        kwargs={'server': args.server})
    heartbeat_thread.start()

    #print("Initializing producer processses...")
    publishers = []
    for n in range(args.num_publishers):
        publishers.append(
                multiprocessing.Process(
                    target=publisher_loop,
                    kwargs={
                        'pub_id': n+1,
                        'num_msgs': int(args.num_msgs/args.num_publishers),
                        'msg_size': args.msg_size, 
                        'num_subscribers': args.num_subscribers,
                        'server': args.server})
                    )
        publishers[n].start()

    # Wait for publisher processes to be established
    publishers_ready = 0

    def pub_ready_callback(channel, method, properties, body):
        nonlocal publishers_ready
        publishers_ready += 1
        if publishers_ready >= args.num_publishers:
            channel.stop_consuming() # publishers are ready

    if publishers_ready < args.num_publishers:
        channel.basic_consume(
            queue=pub_ready_queue_name, on_message_callback=pub_ready_callback, auto_ack=True)
        channel.start_consuming() # wait for publishers to be ready
        
    #print('Waiting for subscriber processes...')
    subscribers = []
    for n in range(args.num_subscribers):
        subscribers.append(
                multiprocessing.Process(
                    target=subscriber_loop,
                    kwargs={
                        'sub_id': n+1,
                        'num_msgs': args.num_msgs,
                        'msg_size': args.msg_size, 
                        'server': args.server})
                    )
        subscribers[n].start()

    #print("Starting Test...")
    
    # Wait for subscribers to finish
    abort_timeout = 120 #seconds
    abort_start = time.perf_counter()

    subscribers_done = 0
    publishers_done = 0

    def sub_done_callback(channel, method, properties, body):
        nonlocal subscribers_done
        subscribers_done += 1
        if (subscribers_done >= args.num_subscribers) and (publishers_done >= args.num_publishers):
            channel.stop_consuming() # publishers and subscribers are done
        elif (time.perf_counter() - abort_start) > abort_timeout: 
            channel.basic_publish(exchange='EXIT', routing_key='', body='')
            sys.stdout.write('Test Timeout! Sending Exit Signal...\n')
            sys.stdout.flush()
            channel.stop_consuming()

    def pub_done_callback(channel, method, properties, body):
        nonlocal publishers_done
        publishers_done += 1
        if (subscribers_done >= args.num_subscribers) and (publishers_done >= args.num_publishers):
            channel.stop_consuming() # publishers and subscribers are done
        elif (time.perf_counter() - abort_start) > abort_timeout: 
            channel.basic_publish(exchange='EXIT', routing_key='', body='')
            sys.stdout.write('Test Timeout! Sending Exit Signal...\n')
            sys.stdout.flush()
            channel.stop_consuming()
    
    def heartbeat_callback(channel, method, properties, body):
        #sys.stdout.write(".")
        #sys.stdout.flush()
        if (time.perf_counter() - abort_start) > abort_timeout: 
            channel.basic_publish(exchange='EXIT', routing_key='', body='')
            sys.stdout.write('Test Timeout! Sending Exit Signal...\n')
            sys.stdout.flush()
            channel.stop_consuming()

    if (subscribers_done < args.num_subscribers) or (publishers_done < args.num_publishers):
        channel.basic_consume(
            queue=sub_done_queue_name, on_message_callback=sub_done_callback, auto_ack=True)
        channel.basic_consume(
            queue=pub_done_queue_name, on_message_callback=pub_done_callback, auto_ack=True)
        channel.basic_consume(
            queue=heartbeat_queue_name, on_message_callback=heartbeat_callback, auto_ack=True)
        channel.start_consuming() # wait for publishers and subscribers to be done

    channel.basic_publish(exchange='EXIT', routing_key='', body='') # send exit to heartbeat thread

    for publisher in publishers:
        publisher.join()

    for subscriber in subscribers:
        subscriber.join()
    
    time.sleep(1) # wait for heartbeat thread to stop
    heartbeat_thread.join()
    
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

    main(args)

    #print('Done!')