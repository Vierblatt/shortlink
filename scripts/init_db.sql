CREATE DATABASE IF NOT EXISTS golink DEFAULT CHARSET utf8mb4;

USE golink;

CREATE TABLE IF NOT EXISTS `users` (
  `id` bigint unsigned NOT NULL,
  `username` varchar(64) NOT NULL COMMENT '用户名',
  `email` varchar(128) NOT NULL COMMENT '邮箱',
  `password` varchar(256) NOT NULL COMMENT '密码(bcrypt)',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_username` (`username`),
  UNIQUE KEY `idx_email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `links` (
  `id` bigint unsigned NOT NULL,
  `short_code` varchar(16) NOT NULL COMMENT '短码',
  `long_url` text NOT NULL COMMENT '原始长链接',
  `user_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT '创建用户ID',
  `expire_at` datetime DEFAULT NULL COMMENT '过期时间',
  `password` varchar(64) DEFAULT NULL COMMENT '访问密码',
  `status` tinyint NOT NULL DEFAULT 1 COMMENT '状态: 0-禁用, 1-启用',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_short_code` (`short_code`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_expire_at` (`expire_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `access_logs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `short_code` varchar(16) NOT NULL COMMENT '短码',
  `ip` varchar(64) NOT NULL COMMENT '访问IP',
  `user_agent` text NOT NULL COMMENT '用户代理',
  `referer` text DEFAULT NULL COMMENT '来源页面',
  `country` varchar(32) DEFAULT NULL COMMENT '国家',
  `province` varchar(32) DEFAULT NULL COMMENT '省份',
  `city` varchar(32) DEFAULT NULL COMMENT '城市',
  `device` varchar(32) DEFAULT NULL COMMENT '设备类型',
  `browser` varchar(32) DEFAULT NULL COMMENT '浏览器',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_short_code` (`short_code`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `link_stats` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `short_code` varchar(16) NOT NULL COMMENT '短码',
  `date` date NOT NULL COMMENT '统计日期',
  `pv` int NOT NULL DEFAULT 0 COMMENT '页面浏览量',
  `uv` int NOT NULL DEFAULT 0 COMMENT '独立访客数',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_short_code_date` (`short_code`, `date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
