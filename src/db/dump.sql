-- we don't know how to generate schema expenses (class Schema) :(

create table matchers
(
	id int auto_increment
		primary key,
	provider varchar(64) collate utf8_bin not null,
	func varchar(64) default 'EQ' null,
	pattern varchar(2048) not null,
	is_category bit default b'0' null
)
;

create table providers
(
	name varchar(32) charset utf8 not null,
	constraint providers_name_uindex
		unique (name)
)
;

alter table providers
	add primary key (name)
;

create table transactions
(
	category text null,
	transaction_time text not null,
	amount double null,
	amount_orig text null,
	operation text null,
	tags text null,
	id int auto_increment
		primary key,
	classified bit default b'0' not null
)
;

create table `values`
(
	value varchar(64) collate utf8_bin not null,
	friendly_name varchar(1024) not null,
	constraint values_value_uindex
		unique (value)
)
;

alter table `values`
	add primary key (value)
;

create table return_values
(
	id int auto_increment
		primary key,
	matcher_id int not null,
	value varchar(64) collate utf8_bin not null,
	constraint return_values_matchers_id_fk
		foreign key (matcher_id) references matchers (id),
	constraint return_values_values_value_fk
		foreign key (value) references `values` (value)
)
;

create table transaction_tags
(
	id int auto_increment
		primary key,
	transaction_id int not null,
	value varchar(64) collate utf8_bin not null,
	constraint transaction_tags_values_value_fk
		foreign key (value) references `values` (value)
)
;

create index transaction_tags_transactions_id_fk
	on transaction_tags (transaction_id)
;

