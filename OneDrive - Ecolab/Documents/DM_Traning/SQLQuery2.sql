create table Product(
ProductID int primary key,
ProductName varchar(100),
Productweight float,
Productprice int,
Productcmp varchar(100));

create table Store(
StoreID varchar(10) primary key,
StoreState varchar(50),
StoreCity varchar(50),
StoreArea varchar(50),
SalesMngr varchar(100));

create table Customer(
Cust_id varchar(10) primary key,
cust_name varchar(100),
cust_email varchar(100),
cust_gender varchar(10)
);

create table Date_Table(
Date date primary key,
Fiscal_year varchar(100),
Year int,
Month varchar(10),
Day varchar(15));

create table sales_fact(
sales_qty int,
sales_amt int,
sales_id varchar(100) not null primary key,
cust_id varchar(10) not null foreign key references Customer(Cust_id),
store_id varchar(10) not null foreign key references Store(StoreID),
prod_id int foreign key references Product(ProductID),
Date date foreign key references Date_Table(Date));

insert into Product(ProductID,ProductName,Productweight,Productprice,Productcmp) values
(1,'Maggi Noodles',100,20,'Nestle'),
(2,'Britannia White Bread',450,50,'Britannia'),
(3,'Lays Chips',78,30,'PepsiCo');

insert into Store(StoreID,StoreState,StoreCity,StoreArea,SalesMngr) values 
('S01','Maharashtra','Mumbai','Andheri','Abhinav G'),
('S02','Maharashtra','Pune','Camp','Dishant S'),
('S03','Karnataka','Bengaluru','Koramanagala','Pamela A'),
('S04','Karnataka','Bengaluru','MG Road','Guruprasad N'),
('S05','Gujarat','Surat','Lalgate','Raju D');

insert into Customer(Cust_id,cust_name ,cust_email ,cust_gender)values
('C001','Anand M','anand.m@gmail.com','M'),
('C002','Pranav D','pranav.d@gmail.com','M'),
('C003','Sisila B','sisila.b@gmail.com','F'),
('C004','Deny O','deny.o@gmail.com','M'),
('C005','Jade N','jade.n@gmail.com','M');

insert into Date_Table(Date,Fiscal_year,Year,Month,Day) values 
('03-03-2023','2022-2023',2023,'March','Friday'),
('04-03-2023','2022-2023',2023,'March','Saturday'),
('05-03-2023','2022-2023',2023,'March','Sunday');

insert into sales_fact(sales_qty ,sales_amt ,sales_id ,cust_id ,store_id ,prod_id ,Date ) values
(2,40,'SL00050','C001','S01',1,'03-03-2023'),
(3,150,'SL00051','C002','S02',2,'04-03-2023'),
(1,30,'SL00052','C003','S03',3,'04-03-2023'),
(2,100,'SL00053','C004','S04',2,'05-03-2023'),
(3,60,'SL00054','C005','S05',1,'05-03-2023');

select * from sales_fact s
inner join Product p on s.prod_id=p.ProductID;