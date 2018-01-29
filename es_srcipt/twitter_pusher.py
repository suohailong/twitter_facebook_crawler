from idatage_export import Espusher
import multiprocessing as mp


if __name__ == '__main__':
    parent_conn, child_conn = mp.Pipe()
    def job():
        es = Espusher()
        print('<<----开始推twitter的文章----->>')
        es.run_twitter_pusher()

    p1 = mp.Process(target=job, args=(child_conn,))
    p1.start()
    while True:
        if parent_conn.poll(20):
            print(parent_conn.recv())
        else:
            print('\n')
            print('程序卡住，重新启动程序')
            print('\n')
            p1.terminate()
            p1 =mp.Process(target=job, args=(child_conn,))
            p1.start()