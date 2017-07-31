CREATE DATABASE `dfs` CHARACTER SET utf8 COLLATE utf8_general_ci;

USE `dfs`;

CREATE TABLE `files` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(255),
    `parent_id` int(10) NOT NULL DEFAULT 0,
    `is_folder` BOOLEAN,
    `size` int(10),
    PRIMARY KEY (`id`),
    UNIQUE KEY `fname` (`name`,`parent_id`)
)
CHARACTER SET utf8 COLLATE utf8_unicode_ci;


CREATE TABLE `storage` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `url` VARCHAR(255),
    `free_space` int(10),
    PRIMARY KEY (`id`)
)
CHARACTER SET utf8 COLLATE utf8_unicode_ci;

CREATE TABLE `file_storage` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `file_id` int(10) unsigned NOT NULL,
    `storage_id` int(10) unsigned NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `fsid` (`file_id`,`storage_id`)
)
CHARACTER SET utf8 COLLATE utf8_unicode_ci;

