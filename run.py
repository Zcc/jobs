from lagouspider import crawllagou
from zhilianspider import crawlzhilian
import multiprocessing

if __name__ == '__main__':
    listenProcess = multiprocessing.Process(target=crawlzhilian)
    listenProcess.start()
    crawllagou()
