# Project_1
Cкрипт, который добавит новый атрибут, полученный из названия файла для всей истории по источнику данных телематики.
Для решения использовал:
- Python
- PySpark для чтения данных из HDFS и записи данных обратно в HDFS.
- subprocess для взаимодействия с HDFS Command Line. Это включало в себя
проверку наличия папок и файлов, а также выполнение операций удаления и
перемещения файлов в HDFS.
- logging добавил логирование для удобной отладки
- использовал конструкцию try...except для обработки исключений, которые могут
возникнуть при выполнении операции чтения списка файлов в указанной директории
из HDFS
