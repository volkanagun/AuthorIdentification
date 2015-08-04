# AuthorIdentification for Turkish

The aim of this project is to classify authors according to written text comming from blog articles, 
news paper articles and tweeter. Project has several stages. First state is WebFlow stage which is collecting articles and generating XML documents from several web resoruces. Second stage is about RDD processing on spark for gathering statistics such as genres, articles types (tweet, blog, and news paper) and author specific statistics such as average length of documents of different types, number of documents from blogs, news papers and tweeter and number of genres for written sources by the author. Third statge is feature extraction and classification.

In WebFlow main function download and XML extraction process begins. In this state predefined regular expression patterns are searched hierarchicaly in HTML text. No DOM is created. Two labels Type and Label are used for 
processing and extraction. Type states how printing to XML will be affected, and label is used to categorize XML tag.
For instance, The SKIP type states where the pattern is not going to be printed in XML but the contents will be. 

In RDDProcessing main function XML documents are processed and document RDD's are formed. The main focus of RDDProcessing is to gather relevant useful code to operate on RDD to extract basic statistics. Later this function will be divided into two where RDDFlow will be instroduced to run jobs as sequencial processes and where RDDProcessing suplies tools for complex operations on RDDS. 
