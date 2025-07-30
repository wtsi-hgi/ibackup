//nolint:gochecknoglobals
package db

var (
	tables = [...]string{
		"CREATE TABLE IF NOT EXISTS `transformers` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`transformer` TEXT NOT NULL, " +
			"`transformerHash` " + hashColumnStart + "`transformer`" + hashColumnEnd + ", " +
			"UNIQUE(`transformerHash`)" +
			");",
		"CREATE TABLE IF NOT EXISTS `sets` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`name` TEXT NOT NULL, " +
			"`nameHash` " + hashColumnStart + "`name`" + hashColumnEnd + ", " +
			"`requester` TEXT NOT NULL, " +
			"`requesterHash` " + hashColumnStart + "`requester`" + hashColumnEnd + ", " +
			"`transformerID` INTEGER, " +
			"`monitorTime` INTEGER NOT NULL, " +
			"`description` TEXT NOT NULL, " +
			"`numFiles` INTEGER DEFAULT 0, " +
			"`sizeFiles` INTEGER DEFAULT 0, " +
			"`uploaded` INTEGER DEFAULT 0, " +
			"`replaced` INTEGER DEFAULT 0, " +
			"`skipped` INTEGER DEFAULT 0, " +
			"`failed` INTEGER DEFAULT 0, " +
			"`missing` INTEGER DEFAULT 0, " +
			"`orphaned` INTEGER DEFAULT 0, " +
			"`abnormal` INTEGER DEFAULT 0, " +
			"`hardlinks` INTEGER DEFAULT 0, " +
			"`symlinks` INTEGER DEFAULT 0, " +
			"`uploadedSize` INTEGER DEFAULT 0, " +
			"`removed` INTEGER DEFAULT 0, " +
			"`removedSize` INTEGER DEFAULT 0, " +
			"`toRemove` INTEGER DEFAULT 0, " +
			"`startedDiscovery` DATETIME DEFAULT '0001-01-01 00:00:00', " +
			"`lastDiscovery` DATETIME DEFAULT '0001-01-01 00:00:00', " +
			"`lastCompleted` DATETIME DEFAULT '0001-01-01 00:00:00', " +
			"`status` TINYINT DEFAULT 0, " +
			"`lastCompletedCount` INTEGER DEFAULT 0, " +
			"`lastCompletedSize` INTEGER DEFAULT 0, " +
			"`error` TEXT NOT NULL, " +
			"`warning` TEXT NOT NULL, " +
			"`metadata` TEXT NOT NULL, " +
			"`reason` TEXT NOT NULL, " +
			"`review` DATETIME DEFAULT '0001-01-01 00:00:00', " +
			"`delete` DATETIME DEFAULT '0001-01-01 00:00:00', " +
			"`deleteLocal` BOOLEAN DEFAULT FALSE, " +
			"`modifiable` BOOLEAN DEFAULT TRUE, " +
			"`hidden` BOOLEAN DEFAULT FALSE, " +
			"UNIQUE(`requesterHash`, `nameHash`), " +
			"FOREIGN KEY(`transformerID`) REFERENCES `transformers`(`id`) ON UPDATE RESTRICT ON DELETE RESTRICT" +
			");",
		"CREATE TABLE IF NOT EXISTS `toDiscover` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`setID` INTEGER NOT NULL, " +
			"`path` TEXT NOT NULL, " +
			"`pathHash` " + hashColumnStart + "`path`" + hashColumnEnd + ", " +
			"`type` TINYINT NOT NULL, " +
			"UNIQUE(`setID`, `pathHash`), " +
			"FOREIGN KEY(`setID`) REFERENCES `sets`(`id`) ON DELETE CASCADE" +
			");",
		"CREATE TABLE IF NOT EXISTS `hardlinks` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`inode` INTEGER NOT NULL, " +
			"`mountpoint` TEXT NOT NULL, " +
			"`mountpointHash` " + hashColumnStart + "`mountpoint`" + hashColumnEnd + ", " +
			"`btime` INTEGER, " +
			"`mtime` INTEGER, " +
			"`size` INTEGER DEFAULT 0, " +
			"`fileType` TINYINT NOT NULL, " +
			"`owner` TEXT NOT NULL, " +
			"`dest` TEXT NOT NULL, " +
			"`remote` TEXT NOT NULL, " +
			"`firstRemote` TEXT NOT NULL, " +
			updateCol +
			"UNIQUE(`mountpointHash`, `inode`, `btime`)" +
			");",
		"CREATE TABLE IF NOT EXISTS `remoteFiles` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`remotePath` TEXT NOT NULL, " +
			"`remotePathHash` " + hashColumnStart + "`remotePath`" + hashColumnEnd + ", " +
			"`lastUploaded` DATETIME DEFAULT '0001-01-01 00:00:00', " +
			"`lastError` TEXT NOT NULL, " +
			"`hardlinkID` INTEGER NOT NULL, " +
			"UNIQUE(`remotePathHash`), " +
			"FOREIGN KEY(`hardlinkID`) REFERENCES `hardlinks`(`id`) ON DELETE RESTRICT" +
			");",
		"CREATE TABLE IF NOT EXISTS `localFiles` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`localPath` TEXT NOT NULL, " +
			"`localPathHash` " + hashColumnStart + "`localPath`" + hashColumnEnd + ", " +
			"`setID` INTEGER NOT NULL, " +
			"`remoteFileID` INTEGER NOT NULL, " +
			"`lastUploaded` DATETIME DEFAULT '0001-01-01 00:00:00', " +
			"`status` TINYINT NOT NULL DEFAULT 0, " +
			"`updated` BOOLEAN DEFAULT FALSE, " +
			"UNIQUE(`localPathHash`, `setID`), " +
			"FOREIGN KEY(`setID`) REFERENCES `sets`(`id`) ON DELETE RESTRICT, " +
			"FOREIGN KEY(`remoteFileID`) REFERENCES `remoteFiles`(`id`) ON DELETE RESTRICT" +
			");",
		"CREATE TABLE IF NOT EXISTS `processes` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`lastPing` DATETIME DEFAULT '0001-01-01 00:00:00'" +
			");",
		"CREATE TABLE IF NOT EXISTS `queue` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`localFileID` INTEGER, " +
			"`type` TINYINT DEFAULT 0," +
			"`attempts` INTEGER DEFAULT 0, " +
			"`lastAttempt` DATETIME DEFAULT '0001-01-01 00:00:00', " +
			"`lastError` TEXT, " +
			"`heldBy` INTEGER, " +
			"`skipped` BOOLEAN DEFAULT FALSE, " +
			"UNIQUE(`localFileID`), " +
			"FOREIGN KEY(`heldBy`) REFERENCES `processes`(`id`) ON DELETE SET NULL, " +
			"FOREIGN KEY(`localFileID`) REFERENCES `localFiles`(`id`) ON DELETE RESTRICT" +
			");",
		"CREATE TRIGGER IF NOT EXISTS `delete_remoteFile_when_not_refd` AFTER DELETE ON `localFiles` FOR EACH ROW BEGIN " +
			"DELETE FROM `remoteFiles` WHERE NOT (" +
			"SELECT COUNT(1) FROM `localFiles` WHERE `localFiles`.`remoteFileID` = `OLD`.`remoteFileID`" +
			") AND `remoteFiles`.`id` = `OLD`.`remoteFileID`;" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `delete_hardlink_when_not_refd` AFTER DELETE ON `remoteFiles` FOR EACH ROW BEGIN " +
			"DELETE FROM `hardlinks` WHERE NOT (" +
			"SELECT COUNT(1) FROM `remoteFiles` WHERE `remoteFiles`.`hardlinkID` = `OLD`.`hardlinkID`" +
			") AND `hardlinks`.`id` = `OLD`.`hardlinkID`;" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `insert_file_count_size` AFTER INSERT ON `localFiles` FOR EACH ROW BEGIN " +
			"/*! WITH `hardlinkInfo` AS (" + newHardlinkInfo + ") */" +
			"UPDATE `sets` SET " +
			"`numFiles` = `numFiles` + 1, " +
			"`sizeFiles` = `sizeFiles` +/*!(SELECT `size` FROM `hardlinkInfo`) -- */`hardlinkInfo`.`size`\n/*! */, " +
			"`symlinks` = `symlinks` + /*! (SELECT `isSymlink` FROM `hardlinkInfo`) -- */ `hardlinkInfo`.`isSymlink`\n/*! */," +
			"`hardlinks` = `hardlinks` +/*!(SELECT `isHardlink` FROM `hardlinkInfo`) -- */`hardlinkInfo`.`isHardlink`\n/*! */," +
			"`abnormal` = `abnormal` + /*! (SELECT `isAbnormal` FROM `hardlinkInfo`) -- */`hardlinkInfo`.`isAbnormal`\n/*! */," +
			"`missing` = `missing` + IF(`NEW`.`status` = " + string('0'+StatusMissing) + ", 1, 0), " +
			"`orphaned` = `orphaned` + IF(`NEW`.`status` = " + string('0'+StatusOrphaned) + ", 1, 0) " +
			"/*! -- */ " +
			"FROM (" + newHardlinkInfo + ") AS `hardlinkInfo`" +
			"\n/*! */ " +
			"WHERE `id` = `NEW`.`setID`;" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `delete_empty_special_sets` AFTER DELETE ON `localFiles` FOR EACH ROW BEGIN " +
			"DELETE FROM `sets` WHERE `id` = `OLD`.`setID` AND " +
			"`numFiles` = 0 AND " +
			"(`name` LIKE CONCAT(CHAR(0), '%') OR `requester` LIKE CONCAT(CHAR(0), '%'));" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `delete_file_count_size` AFTER DELETE ON `localFiles` FOR EACH ROW BEGIN " +
			"/*! WITH `hardlinkInfo` AS (" + oldHardlinkInfo + ") */" +
			"UPDATE `sets` SET " +
			"`numFiles` = `numFiles` - 1, " +
			"`sizeFiles` = `sizeFiles` -/*!(SELECT `size` FROM `hardlinkInfo`) -- */`hardlinkInfo`.`size`\n/*! */, " +
			"`symlinks` = `symlinks` - /*! (SELECT `isSymlink` FROM `hardlinkInfo`) -- */ `hardlinkInfo`.`isSymlink`\n/*! */," +
			"`hardlinks` = `hardlinks` -/*!(SELECT `isHardlink` FROM `hardlinkInfo`) -- */`hardlinkInfo`.`isHardlink`\n/*! */," +
			"`abnormal` = `abnormal` - /*! (SELECT `isAbnormal` FROM `hardlinkInfo`) -- */`hardlinkInfo`.`isAbnormal`\n/*! */," +
			"`missing` = `missing` - IF(`OLD`.`status` = " + string('0'+StatusMissing) + ", 1, 0), " +
			"`orphaned` = `orphaned` - IF(`OLD`.`status` = " + string('0'+StatusOrphaned) + ", 1, 0) " +
			"/*! -- */ " +
			"FROM (" + oldHardlinkInfo + ") AS `hardlinkInfo`" +
			"\n/*! */ " +
			"WHERE `id` = `OLD`.`setID`;" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `update_file_size` AFTER UPDATE ON `hardlinks` FOR EACH ROW BEGIN " +
			"UPDATE `sets` SET `sizeFiles` = `sizeFiles` - `OLD`.`size` + `NEW`.`size` WHERE `id` IN (" +
			"SELECT `localFiles`.`setID` FROM `localFiles` " +
			"JOIN `remoteFiles` ON `remoteFiles`.`id` = `localFiles`.`remoteFileID` WHERE " +
			"`remoteFiles`.`hardlinkID` = `NEW`.`id`);" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `upload_local_file` AFTER INSERT ON `localFiles` FOR EACH ROW BEGIN " +
			"INSERT INTO `queue` (`localFileID`, `type`) SELECT `NEW`.`id`, " + string('0'+QueueUpload) + " " +
			"WHERE `NEW`.`status` NOT IN (" + string('0'+StatusMissing) + ", " + string('0'+StatusOrphaned) + ") " +
			onConflictUpdate + "`type` = " + string('0'+QueueUpload) + ", `attempts` = 0, " +
			"`lastAttempt` = '0001-01-01 00:00:00', `lastError` = '';" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `update_set_counts_on_file_update` AFTER UPDATE ON `localFiles` FOR EACH ROW BEGIN " +
			"UPDATE `sets` SET " +
			"`uploaded` = `uploaded` + IF(`OLD`.`status` = " + string('0'+StatusUploaded) + ", " +
			"IF(`NEW`.`status` = " + string('0'+StatusUploaded) + ", 0, -1), " +
			"IF(`NEW`.`status` = " + string('0'+StatusUploaded) + ", 1, 0)), " +
			"`replaced` = `replaced` + IF(`OLD`.`status` = " + string('0'+StatusReplaced) + ", " +
			"IF(`NEW`.`status` = " + string('0'+StatusReplaced) + ", 0, -1), " +
			"IF(`NEW`.`status` = " + string('0'+StatusReplaced) + ", 1, 0)), " +
			"`skipped` = `skipped` + IF(`OLD`.`status` = " + string('0'+StatusSkipped) + ", " +
			"IF(`NEW`.`status` = " + string('0'+StatusSkipped) + ", 0, -1), " +
			"IF(`NEW`.`status` = " + string('0'+StatusSkipped) + ", 1, 0)), " +
			"`missing` = `missing` + IF(`OLD`.`status` = " + string('0'+StatusMissing) + ", " +
			"IF(`NEW`.`status` = " + string('0'+StatusMissing) + ", 0, -1), " +
			"IF(`NEW`.`status` = " + string('0'+StatusMissing) + ", 1, 0)), " +
			"`orphaned` = `orphaned` + IF(`OLD`.`status` = " + string('0'+StatusOrphaned) + ", " +
			"IF(`NEW`.`status` = " + string('0'+StatusOrphaned) + ", 0, -1), " +
			"IF(`NEW`.`status` = " + string('0'+StatusOrphaned) + ", 1, 0)) " +
			"WHERE `sets`.`id` = `NEW`.`setID`;" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `reupload_local_file` AFTER UPDATE ON `localFiles` FOR EACH ROW BEGIN " +
			"/*! IF `OLD`.`updated` != `NEW`.`updated` THEN */" +
			"INSERT INTO `queue` (`localFileID`, `type`) SELECT `NEW`.`id`, " + string('0'+QueueUpload) + " " +
			"WHERE `OLD`.`updated` != `NEW`.`updated` AND " +
			"`NEW`.`status` NOT IN (" + string('0'+StatusMissing) + ", " + string('0'+StatusOrphaned) + ") " +
			onConflictUpdate + "`type` = " + string('0'+QueueUpload) + ", `attempts` = 0, " +
			"`lastAttempt` = '0001-01-01 00:00:00', `lastError` = '';" +
			"/*! END IF; */" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `update_set_toRemove_when_queued` AFTER INSERT ON `queue` FOR EACH ROW BEGIN " +
			"/*! IF `NEW`.`type` = " + string('0'+QueueRemoval) + " THEN */" +
			"UPDATE `sets` SET " +
			"`removedSize` = IF(`toRemove` = `removed`, 0, `removedSize`), " +
			"`toRemove` = 1 + IF(`toRemove` = `removed`, 0, `toRemove`), " +
			"`removed` = IF(`toRemove` = `removed`, 0, `removed`) " +
			"WHERE `NEW`.`type` = " + string('0'+QueueRemoval) + " AND " +
			"`id` = (SELECT `setID` FROM `localFiles` WHERE `id` = `NEW`.`localFileID`);" +
			"/*! END IF; */" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `update_set_after_queued_error` AFTER UPDATE ON `queue` FOR EACH ROW BEGIN " +
			"/*! IF `OLD`.`lastError` IS NULL AND `NEW`.`lastError` IS NOT NULL THEN */" +
			"UPDATE `sets` SET " +
			"`failed` = `failed` + 1 " +
			"WHERE `OLD`.`lastError` IS NULL AND `NEW`.`lastError` IS NOT NULL AND " +
			"`sets`.`id` = (SELECT `setID` FROM `localFiles` WHERE `localFiles`.`id` = `OLD`.`localfileID`);" +
			"/*! END IF; */" +
			"/*!IF `OLD`.`type` = " + string('0'+QueueRemoval) + " AND `NEW`.`type` != " + string('0'+QueueRemoval) + " THEN*/" +
			"UPDATE `sets` SET " +
			"`toRemove` = `toRemove` - 1 " +
			"WHERE `OLD`.`type` = " + string('0'+QueueRemoval) + " AND `NEW`.`type` != " + string('0'+QueueRemoval) + " AND " +
			"`sets`.`id` = (SELECT `setID` FROM `localFiles` WHERE `localFiles`.`id` = `OLD`.`localfileID`);" +
			"/*! END IF; */" +
			"/*!IF `OLD`.`type` != " + string('0'+QueueRemoval) + " AND `NEW`.`type` = " + string('0'+QueueRemoval) + " THEN*/" +
			"UPDATE `sets` SET " +
			"`toRemove` = `toRemove` + 1 " +
			"WHERE `OLD`.`type` != " + string('0'+QueueRemoval) + " AND `NEW`.`type` = " + string('0'+QueueRemoval) + " AND " +
			"`sets`.`id` = (SELECT `setID` FROM `localFiles` WHERE `localFiles`.`id` = `OLD`.`localfileID`);" +
			"/*! END IF; */" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `update_file_after_queued_action` AFTER DELETE ON `queue` FOR EACH ROW BEGIN " +
			"/*! IF `OLD`.`lastError` IS NOT NULL THEN */" +
			"UPDATE `sets` SET " +
			"`failed` = `failed` - 1 " +
			"WHERE `OLD`.`lastError` IS NOT NULL AND " +
			"`sets`.`id` = (SELECT `setID` FROM `localFiles` WHERE `localFiles`.`id` = `OLD`.`localfileID`);" +
			"/*! END IF; */" +
			"/*! IF `OLD`.`type` = " + string('0'+QueueRemoval) + " THEN */" +
			"UPDATE `sets` SET " +
			"`removed` = `removed` + 1, " +
			"`removedSize` = `removedSize` + (" +
			"SELECT `hardlinks`.`size` FROM `localFiles` JOIN " +
			"`remoteFiles` ON `localFiles`.`remoteFileID` = `remoteFiles`.`id` JOIN " +
			"`hardlinks` ON `remoteFiles`.`hardlinkID` = `hardlinks`.`id` WHERE " +
			"`localFiles`.`id` = `OLD`.`localFileID`) " +
			"WHERE `OLD`.`type` = " + string('0'+QueueRemoval) + " AND " +
			"`id` = (SELECT `setID` FROM `localFiles` WHERE `id` = `OLD`.`localFileID`);" +
			"DELETE FROM `localFiles` WHERE " +
			"`OLD`.`type` = " + string('0'+QueueRemoval) + " AND `localFiles`.`id` = `OLD`.`localFileID`;" +
			"/*! END IF; */" +
			"/*! IF `OLD`.`type` = " + string('0'+QueueUpload) + " THEN */ " +
			"UPDATE `sets` " +
			"/*! JOIN `localFiles` ON `OLD`.`localFileID` = `localFiles`.`id` JOIN " +
			"`remoteFiles` ON `localFiles`.`remoteFileID` = `remoteFiles`.`id` JOIN " +
			"`hardlinks` ON `remoteFiles`.`hardlinkID` = `hardlinks`.`id` */ " +
			"SET " +
			"`uploadedSize` = `uploadedSize` + `hardlinks`.`size` " +
			"/*! -- */ FROM `localFiles` JOIN " +
			"`remoteFiles` ON `localFiles`.`remoteFileID` = `remoteFiles`.`id` JOIN " +
			"`hardlinks` ON `remoteFiles`.`hardlinkID` = `hardlinks`.`id`\n/*! */" +
			"WHERE `OLD`.`type` = " + string('0'+QueueUpload) + " AND " +
			"`OLD`.`skipped` = FALSE AND " +
			"`sets`.`id` = `localFiles`.`setID` AND " +
			"`localFiles`.`id` = `OLD`.`localFileID`;" +
			"UPDATE `localFiles` /*! JOIN `remoteFiles` ON `localFiles`.`remoteFileID` = `remoteFiles`.`id` */ SET " +
			"/*! `localFiles`.*/`lastUploaded` = IF(`OLD`.`skipped`, `localFiles`.`lastUploaded`, " + now + "), " +
			"`status` = IF(`OLD`.`skipped`, " + string('0'+StatusSkipped) + ", " +
			"IF(`remoteFiles`.`lastUploaded` = '0001-01-01 00:00:00', " + string('0'+StatusUploaded) + ", " +
			string('0'+StatusReplaced) + ")) " +
			"/*! -- */ FROM `localFiles` AS `local` JOIN `remoteFiles` ON `local`.`remoteFileID` = `remoteFiles`.`id`\n/*! */" +
			"WHERE `OLD`.`type` = " + string('0'+QueueUpload) + " AND " +
			"`localFiles`.`id` = `OLD`.`localFileID` AND " +
			"`localFiles`.`status` NOT IN (" + string('0'+StatusMissing) + ", " + string('0'+StatusOrphaned) + ");" +
			"/*! IF `OLD`.`skipped` = FALSE THEN */ " +
			"UPDATE `remoteFiles` SET `lastUploaded` = " + now + " " +
			"WHERE `OLD`.`skipped` = FALSE AND `OLD`.`type` = " + string('0'+QueueUpload) + " AND " +
			"`remoteFiles`.`id` = (SELECT `remoteFileID` from `localFiles` WHERE `localFiles`.`id` = `OLD`.`localFileID`);" +
			"/*! WITH `remoteFilesInfo` AS (SELECT `remoteFileID` FROM `localFiles` WHERE `id` = `OLD`.`localFileID`) */" +
			"UPDATE `localFiles` SET " +
			"`status` = " + string('0'+StatusOrphaned) + " " +
			"WHERE `OLD`.`type` = " + string('0'+QueueUpload) + " AND " +
			"`status` = " + string('0'+StatusMissing) + " AND " +
			"`remoteFileID` = (" +
			"SELECT `remoteFileID` FROM /*! `remoteFilesInfo` -- */`localFiles` WHERE `id`= `OLD`.`localFileID`\n/*! */);" +
			"/*! END IF; */" +
			"/*! END IF; */" +
			"END;",
	}
)

const (
	virtStart       = "/*! UNHEX(SHA2(*/"
	virtEnd         = "/*!, 0))*/"
	virtPosition    = virtStart + "?" + virtEnd
	hashColumnStart = "/*! VARBINARY(32) -- */ TEXT\n/* */GENERATED ALWAYS AS (" + virtStart
	hashColumnEnd   = virtEnd + ") VIRTUAL /*! INVISIBLE */"
	now             = "/*! NOW() -- */ DATETIME('now')\n/*! */"
	setRef          = "/*! AS `EXCLUDED` */ "
	updateCol       = "/*! `updated` BOOLEAN DEFAULT FALSE, */"
	colUpdate       = " /*!`updated` = ! `updated`, */ "
	hardlinkInfo    = "SELECT " +
		"`hardlinks`.`size`, " +
		"IF(`hardlinks`.`fileType` = " + string('0'+Symlink) + ", 1, 0) AS `isSymlink`, " +
		"IF(`hardlinks`.`fileType` = " + string('0'+Abnormal) + ", 1, 0) AS `isAbnormal`, " +
		"IF(`hardlinks`.`fileType` = " + string('0'+Regular) + " AND " +
		"`remoteFiles`.`remotePath` != `hardlinks`.`firstRemote`, 1, 0) AS `isHardlink`, " +
		"IF(`remoteFiles`.`lastUploaded` = '0001-01-01 00:00:00', 0, 1) AS `isUploaded` " +
		"FROM `hardlinks` JOIN `remoteFiles` ON `remoteFiles`.`hardlinkID` = `hardlinks`.`id` " +
		"WHERE `remoteFiles`.`id` = "
	newHardlinkInfo = hardlinkInfo + "`NEW`.`remoteFileID`"
	oldHardlinkInfo = hardlinkInfo + "`OLD`.`remoteFileID`"
	localFileCounts = "SELECT " +
		"COUNT(1) AS `count`, " +
		"`setID` " +
		"FROM `localFiles` WHERE `remoteFileID` = `NEW`.`id` GROUP BY `setID`"

	onConflictUpdate   = "ON /*! DUPLICATE KEY UPDATE -- */ CONFLICT DO UPDATE SET\n/*! */ "
	onConflictReturnID = "ON /*! DUPLICATE KEY UPDATE `id` = LAST_INSERT_ID(`id`); -- */ " +
		"CONFLICT DO UPDATE SET `id` = `id` RETURNING `id`;\n/*! */"
	returnOrSetID = " /*! `id` = LAST_INSERT_ID(`id`); -- */ `id` = `id` RETURNING `id`;\n/*! */"

	createTransformer = "INSERT INTO `transformers` (" +
		"`transformer`" +
		") VALUES (?) " + onConflictReturnID
	createSet = "INSERT INTO `sets` (" +
		"`name`, " +
		"`requester`, " +
		"`transformerID`, " +
		"`monitorTime`, " +
		"`description`, " +
		"`error`, " +
		"`warning`, " +
		"`metadata`, " +
		"`reason`, " +
		"`review`, " +
		"`delete` " +
		") VALUES (?, ?, ?, ?, ?, '', '', ?, ?, ?, ?);"
	createTrashSetForFile = "INSERT INTO `sets` (" +
		"`name`, " +
		"`requester`, " +
		"`transformerID`, " +
		"`monitorTime`, " +
		"`description`, " +
		"`error`, " +
		"`warning`, " +
		"`metadata`, " +
		"`reason` " +
		") " +
		"SELECT " +
		"CONCAT(CHAR(0), `oldSet`.`name`), " +
		"`oldSet`.`requester`, " +
		"`oldSet`.`transformerID`, " +
		"/*!LAST_INSERT_ID(*/0/*!)*/, " +
		"'null', '', '', '', '' " +
		"FROM `localFiles` JOIN `sets` AS `oldSet` ON `localFiles`.`setID` = `oldSet`.`id` " +
		"WHERE `localFiles`.`id` = ? AND `oldSet`.`name` NOT LIKE CONCAT(CHAR(0), '%') " +
		"ON /*! DUPLICATE KEY UPDATE `sets`.`id` = LAST_INSERT_ID(`sets`.`id`); -- */ " +
		"CONFLICT DO UPDATE SET `id` = `sets`.`id` RETURNING `sets`.`id`;\n/*! */"
	createHardlink = "INSERT INTO `hardlinks` (" +
		"`inode`, " +
		"`mountpoint`, " +
		"`btime`, " +
		"`remote`, " +
		"`mtime`, " +
		"`size`, " +
		"`fileType`, " +
		"`owner`, " +
		"`dest`, " +
		"`firstRemote`" +
		") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " + setRef +
		onConflictUpdate + colUpdate + "`mtime` = `EXCLUDED`.`mtime`, `dest` = `EXCLUDED`.`dest`, " +
		"`size` = `EXCLUDED`.`size`, `owner` = `EXCLUDED`.`owner`, " +
		returnOrSetID
	createRemoteFile = "INSERT INTO `remoteFiles` (" +
		"`remotePath`, " +
		"`hardlinkID`, " +
		"`lastError`" +
		") VALUES (?, ?, '') " + onConflictReturnID
	createSetFile = "INSERT INTO `localFiles` (" +
		"`localPath`, " +
		"`setID`, " +
		"`remoteFileID`, " +
		"`status`" +
		") VALUES (?, ?, ?, " +
		"IF(? IN (" + string('0'+StatusMissing) + ", " + string('0'+StatusOrphaned) + "), " +
		"IF((SELECT `lastUploaded` FROM `remoteFiles` WHERE `id` = ?) != '0001-01-01 00:00:00', " +
		string('0'+StatusOrphaned) + ", " +
		string('0'+StatusMissing) + "), " +
		"0)" +
		") " + setRef +
		onConflictUpdate +
		"`status` = `EXCLUDED`.`status`, " +
		"`updated` = NOT `updated`, " + returnOrSetID
	createTrashFile = "INSERT INTO `localFiles` (" +
		"`localPath`, " +
		"`setID`, " +
		"`remoteFileID`, " +
		"`status`" +
		") " +
		"SELECT `localPath`, ?, `remoteFileID`, `status` FROM `localFiles` AS `oldFile` WHERE `oldFile`.`id` = ? " +
		"ON /*! DUPLICATE KEY UPDATE `localFiles`.`id` = LAST_INSERT_ID(`localFiles`.`id`); -- */ " +
		"CONFLICT DO UPDATE SET `id` = `localFiles`.`id` RETURNING `localFiles`.`id`;\n/*! */"
	createDiscover = "INSERT INTO `toDiscover` (" +
		"`setID`, " +
		"`path`, " +
		"`type`" +
		") VALUES (?, ?, ?) " + setRef + onConflictUpdate + "`type` = `EXCLUDED`.`type`;"
	createQueuedRemoval = "INSERT INTO `queue` (" +
		"`localFileID`, " +
		"`type`" +
		") VALUES (?, " + string('0'+QueueRemoval) + ") " + onConflictUpdate +
		"`type` = " + string('0'+QueueRemoval) + ", `attempts` = 0, `lastAttempt` = '0001-01-01 00:00:00', `lastError` = '';"
	createQueuedRemovalForSet = "INSERT INTO `queue` (" +
		"`localFileID`, " +
		"`type`" +
		") SELECT `localFiles`.`id`, " + string('0'+QueueRemoval) + " FROM `localFiles` WHERE `localFiles`.`setID` = ? " +
		onConflictUpdate +
		"`type` = " + string('0'+QueueRemoval) + ", `attempts` = 0, `lastAttempt` = '0001-01-01 00:00:00', `lastError` = '';"
	createProcess = "INSERT INTO `processes` (`lastPing`) VALUES (" + now + ");"

	getSetsStart = "SELECT " +
		"`sets`.`id`, " +
		"`sets`.`name`, " +
		"`sets`.`requester`, " +
		"`sets`.`description`, " +
		"`sets`.`monitorTime`, " +
		"`sets`.`metadata`, " +
		"`sets`.`reason`, " +
		"`sets`.`review`, " +
		"`sets`.`delete`, " +
		"`sets`.`numFiles`," +
		"`sets`.`sizeFiles`," +
		"`sets`.`uploaded`," +
		"`sets`.`replaced`," +
		"`sets`.`skipped`," +
		"`sets`.`failed`," +
		"`sets`.`missing`," +
		"`sets`.`orphaned`," +
		"`sets`.`abnormal`," +
		"`sets`.`hardlinks`," +
		"`sets`.`symlinks`," +
		"`sets`.`uploadedSize`," +
		"`sets`.`removed`," +
		"`sets`.`removedSize`," +
		"`sets`.`toRemove`," +
		"`sets`.`startedDiscovery`, " +
		"`sets`.`lastDiscovery`, " +
		"`sets`.`status`, " +
		"`sets`.`lastCompletedCount`, " +
		"`sets`.`lastCompletedSize`, " +
		"`sets`.`error`, " +
		"`sets`.`warning`, " +
		"`sets`.`modifiable`, " +
		"`sets`.`hidden`, " +
		"`transformers`.`transformer` " +
		"FROM `sets` JOIN `transformers` ON `sets`.`transformerID` = `transformers`.`id`"
	getAllSets = getSetsStart + " WHERE " +
		"`sets`.`name` NOT LIKE CONCAT(CHAR(0), '%') AND " +
		"`sets`.`requester` NOT LIKE CONCAT(CHAR(0), '%') " +
		"ORDER BY `sets`.`id` ASC;"
	getSetByNameRequester = getSetsStart +
		" WHERE `sets`.`nameHash` = " + virtPosition + " and `sets`.`requesterHash` = " + virtPosition + ";"
	getSetByID         = getSetsStart + " WHERE `sets`.`id` = ?;"
	getSetsByRequester = getSetsStart +
		" WHERE `sets`.`requesterHash` = " + virtPosition + " ORDER BY `sets`.`id` ASC;"
	getSetsFilesStart = "SELECT " +
		"`localFiles`.`id`, " +
		"`localFiles`.`localPath`, " +
		"`localFiles`.`lastUploaded`, " +
		"`localFiles`.`status`, " +
		"`remoteFiles`.`remotePath`, " +
		"`hardlinks`.`size`, " +
		"`hardlinks`.`fileType`, " +
		"`hardlinks`.`owner`, " +
		"`hardlinks`.`inode`, " +
		"`hardlinks`.`mountPoint`, " +
		"`hardlinks`.`btime`, " +
		"`hardlinks`.`mtime`, " +
		"`hardlinks`.`remote`, " +
		"`hardlinks`.`dest` "
	getSetsFilesFrom = "FROM `localFiles` " +
		"JOIN `remoteFiles` ON `localFiles`.`remoteFileID` = `remoteFiles`.`id` " +
		"JOIN `hardlinks` ON `remoteFiles`.`hardlinkID` = `hardlinks`.`id` "
	getSetsFilesWhere      = "WHERE `localFiles`.`setID` = ? ORDER BY `localFiles`.`id` ASC;"
	getSetsFiles           = getSetsFilesStart + getSetsFilesFrom + getSetsFilesWhere
	getSetsFilesWithErrors = getSetsFilesStart + ", " +
		"COALESCE(`queue`.`lastError`, ''), " +
		"`queue`.`lastAttempt`, " +
		"COALESCE(`queue`.`attempts`, 0) " +
		getSetsFilesFrom +
		"LEFT JOIN `queue` ON `localFiles`.`id` = `queue`.`localFileID` " +
		getSetsFilesWhere
	getSetDiscovery = "SELECT " +
		"`path`, " +
		"`type`" +
		" FROM `toDiscover` WHERE `setID` = ? ORDER BY `id` ASC;"
	getQueuedTasks = "SELECT " +
		"`queue`.`id`, " +
		"`queue`.`type`, " +
		"`localFiles`.`localPath`, " +
		"`remoteFiles`.`remotePath`, " +
		"`hardlinks`.`firstRemote` " +
		"FROM `queue` " +
		"JOIN `localFiles` ON `localFiles`.`id` = `queue`.`localFileID` " +
		"JOIN `remoteFiles` ON `remoteFiles`.`id` = `localFiles`.`remoteFileID` " +
		"JOIN `hardlinks` ON `hardlinks`.`id` = `remoteFiles`.`hardlinkID` " +
		"WHERE `queue`.`heldBy` = ?;"
	getRemoteFileRefs = "SELECT COUNT(1) FROM `localFiles` WHERE `remoteFileID` = (" +
		"SELECT `remoteFileID` FROM `localFiles` WHERE `localPathHash` = " + virtStart +
		"(SELECT `localPath` FROM `localFiles` WHERE `id` = ? LIMIT 1)" +
		virtEnd + ");"
	getTasksCounts  = "SELECT COUNT(1), COUNT(`heldBy`) FROM `queue`;"
	getProcessCount = "SELECT COUNT(1) FROM `processes`;"

	updateSetWarning             = "UPDATE `sets` SET `warning` = ? WHERE `id` = ?;"
	updateSetError               = "UPDATE `sets` SET `warning` = ? WHERE `id` = ?;"
	updateSetReadonly            = "UPDATE `sets` SET `modifiable` = FALSE WHERE `id` = ?;"
	updateSetModifiable          = "UPDATE `sets` SET `modifiable` = TRUE WHERE `id` = ?;"
	updateSetHidden              = "UPDATE `sets` SET `hidden` = TRUE WHERE `id` = ?;"
	updateSetVisible             = "UPDATE `sets` SET `hidden` = FALSE WHERE `id` = ?;"
	updateDiscoveryStarted       = "UPDATE `sets` SET `startedDiscovery` = " + now + " WHERE `id` = ?;"
	updateLastDiscoveryCompleted = "UPDATE `sets` SET " +
		"`lastDiscovery` = `startedDiscovery`, " +
		"`lastCompleted` = " + now + ", " +
		"`lastCompletedCount` = `numFiles`, " +
		"`lastCompletedSize` = `sizeFiles`, " +
		"`uploaded` = 0, `uploadedSize` = 0, `replaced` = 0, `skipped` = 0 " +
		"WHERE `id` = ?;"
	updateDiscoverySet = "UPDATE `toDiscover` SET `setID` = ? WHERE `setID` = ?;"
	updateQueuedFailed = "UPDATE `queue` SET " +
		"`attempts` = `attempts` + 1, " +
		"`lastError` = ?, " +
		"`lastAttempt` = " + now + ", " +
		"`heldBy` = NULL " +
		"WHERE `id` = ? AND `heldBy` = ? AND `type` = ?;"
	updateQueuedSkipped = "UPDATE `queue` SET " +
		"`skipped` = TRUE WHERE `id` = ? AND `heldBy` = ? AND `type` = ?;"
	updateProcessPing = "UPDATE `processes SET `lastPing` = " + now + " WHERE `id` = ?;"

	shiftSetRequester = "UPDATE `sets` SET " +
		"`requester` = CONCAT(CHAR(0), `id`, CHAR(0), `requester`) " +
		"WHERE `id` = ? AND `requester` NOT LIKE CONCAT(CHAR(0), '%');"

	holdQueuedTask = "WITH " +
		"`heldHardlinks` AS (" +
		"SELECT `remoteFiles`.`hardlinkID` FROM `queue` " +
		"JOIN `localFiles` ON `queue`.`localFileID` = `localFiles`.`id` " +
		"JOIN `remoteFiles` ON `localFiles`.`remoteFileID` = `remoteFiles`.`id` " +
		"WHERE `queue`.`heldBy` IS NOT NULL" +
		"), " +
		"`available` AS (" +
		"SELECT `queue`.`id` FROM `queue` " +
		"JOIN `localFiles` ON `queue`.`localFileID` = `localFiles`.`id` " +
		"JOIN `remoteFiles` ON `localFiles`.`remoteFileID` = `remoteFiles`.`id` " +
		"WHERE `remoteFiles`.`hardlinkID` NOT IN (SELECT `hardlinkID` FROM `heldHardlinks`) " +
		"ORDER BY `queue`.`attempts` ASC, `queue`.`id` ASC LIMIT 1" +
		") " +
		"UPDATE `queue` SET `heldBy` = ? WHERE " +
		"`queue`.`id` IN (SELECT `id` FROM `available`) AND `queue`.`attempts` < " + string('0'+maxRetries) + ";"
	releaseQueuedTask = "UPDATE `queue` SET `heldBy` = NULL WHERE `heldBy` = ?;"

	queueRetry = "WITH " +
		"`setFiles` AS (" +
		"SELECT `id` FROM `localFiles` " +
		"WHERE `setID` = ?" +
		") " +
		"UPDATE `queue` SET `attempts` = 0 " +
		"WHERE `localFileID` IN (SELECT `id` FROM `setFiles`);"

	disableTrashFileRemoveTask = "UPDATE `queue` SET " +
		"`type` = " + string('0'+QueueDisabled) + " " +
		"WHERE `localFileID` = ? AND `type` = " + string('0'+QueueRemoval) + ";"

	deleteSet            = "DELETE FROM `sets` WHERE `id` = ?;"
	deleteDiscover       = "DELETE FROM `toDiscover` WHERE `setID` = ? AND `path` = ?;"
	deleteQueued         = "DELETE FROM `queue` WHERE `id` = ? AND `heldBy` = ? AND `type` = ?;"
	deleteStaleProcesses = "DELETE FROM `processes` WHERE `lastPing` < /*! NOW() - INTERVAL 10 MINUTE -- */ " +
		"DATETIME('now', '-10 MINUTES')\n/*! */;"
)
