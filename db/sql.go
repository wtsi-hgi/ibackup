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
			"`startedDiscovery` DATETIME DEFAULT \"0001-01-01 00:00:00\", " +
			"`lastDiscovery` DATETIME DEFAULT \"0001-01-01 00:00:00\", " +
			"`lastCompleted` DATETIME DEFAULT \"0001-01-01 00:00:00\", " +
			"`status` TINYINT DEFAULT 0, " +
			"`lastCompletedCount` INTEGER DEFAULT 0, " +
			"`lastCompletedSize` INTEGER DEFAULT 0, " +
			"`error` TEXT NOT NULL, " +
			"`warning` TEXT NOT NULL, " +
			"`metadata` TEXT NOT NULL, " +
			"`deleteLocal` BOOLEAN DEFAULT FALSE, " +
			"`readonly` BOOLEAN DEFAULT FALSE, " +
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
			"`size` INTEGER NOT NULL, " +
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
			"`lastUploaded` DATETIME DEFAULT \"0001-01-01 00:00:00\", " +
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
			"`lastUpload` DATETIME DEFAULT \"0001-01-01 00:00:00\", " +
			"UNIQUE(`localPathHash`, `setID`), " +
			"FOREIGN KEY(`setID`) REFERENCES `sets`(`id`) ON DELETE CASCADE, " +
			"FOREIGN KEY(`remoteFileID`) REFERENCES `remoteFiles`(`id`) ON DELETE RESTRICT" +
			");",
		"CREATE TABLE IF NOT EXISTS `processes` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`lastPing` DATETIME DEFAULT \"0001-01-01 00:00:00\"" +
			");",
		"CREATE TABLE IF NOT EXISTS `queue` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`localFileID` INTEGER, " +
			"`type` TINYINT DEFAULT 0," +
			"`attempts` INTEGER DEFAULT 0, " +
			"`lastAttempt` DATETIME DEFAULT \"0001-01-01 00:00:00\", " +
			"`lastError` TEXT, " +
			"`heldBy` INTEGER DEFAULT 0, " +
			"UNIQUE(`localFileID`), " +
			"/*! INDEX(`heldBy`), */" +
			"FOREIGN KEY(`localFileID`) REFERENCES `localFiles`(`id`) ON DELETE RESTRICT" +
			");",
		"/*! -- */ CREATE INDEX IF NOT EXISTS `queueHeldBy` ON `queue`(`heldBy`);\n/*! */",
		"CREATE TRIGGER IF NOT EXISTS `insert_file_count_size` AFTER INSERT ON `localFiles` FOR EACH ROW BEGIN " +
			"UPDATE `sets` SET `numFiles` = `numFiles` + 1, `sizeFiles` = `sizeFiles` + (" +
			"SELECT `hardlinks`.`size` FROM `hardlinks` JOIN `remoteFiles` ON `remoteFiles`.`hardlinkID` = `hardlinks`.`id` " +
			"WHERE `remoteFiles`.`id` = `NEW`.`remoteFileID`) " +
			"WHERE `id` = `NEW`.`setID`; END;",
		"CREATE TRIGGER IF NOT EXISTS `delete_file_count_size` AFTER DELETE ON `localFiles` FOR EACH ROW BEGIN " +
			"UPDATE `sets` SET `numFiles` = `numFiles` - 1, `sizeFiles` = `sizeFiles` - (" +
			"SELECT `hardlinks`.`size` FROM `hardlinks` JOIN `remoteFiles` ON `remoteFiles`.`hardlinkID` = `hardlinks`.`id` " +
			"WHERE `remoteFiles`.`id` = `OLD`.`remoteFileID`) " +
			"WHERE `id` = `OLD`.`setID`;" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `update_file_size` AFTER UPDATE ON `hardlinks` FOR EACH ROW BEGIN " +
			"UPDATE `sets` SET `sizeFiles` = `sizeFiles` - `OLD`.`size` + `NEW`.`size` WHERE `id` IN (" +
			"SELECT `localFiles`.`setID` FROM `localFiles` " +
			"JOIN `remoteFiles` ON `remoteFiles`.`id` = `localFiles`.`remoteFileID` WHERE " +
			"`remoteFiles`.`hardlinkID` = `NEW`.`id`);" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `upload_local_file` AFTER INSERT ON `localFiles` FOR EACH ROW BEGIN " +
			"INSERT INTO `queue` (`localFileID`, `type`) VALUES (`NEW`.`id`, " + string('0'+QueueUpload) + ") " +
			onConflictUpdate + "`type` = " + string('0'+QueueUpload) + ", `attempts` = 0, " +
			"`lastAttempt` = '0001-01-01 00:00:00', `lastError` = '';" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `update_file_after_queued_action` AFTER DELETE ON `queue` FOR EACH ROW BEGIN " +
			"DELETE FROM `localFiles` WHERE " +
			"`OLD`.`type` = " + string('0'+QueueRemoval) + " AND `localFiles`.`id` = `OLD`.`localFileID`;" +
			"UPDATE `remoteFiles` SET `lastUploaded` = " + now + " " +
			"WHERE `OLD`.`type` = " + string('0'+QueueUpload) + " AND " +
			"`remoteFiles`.`id` IN (SELECT `remoteFileID` from `localFiles` WHERE `localFiles`.`id` = `OLD`.`localFileID`);" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `relase_held_jobs` AFTER DELETE ON `processes` FOR EACH ROW BEGIN " +
			"UPDATE `queue` SET `heldBy` = 0 WHERE `heldBy` = `OLD`.`id`;" +
			"END;",
	}
)

const (
	virtStart       = "/*! UNHEX(SHA2(*/"
	virtEnd         = "/*!, 0))*/"
	virtPosition    = virtStart + "?" + virtEnd
	hashColumnStart = "/*! VARBINARY(32) -- */ TEXT\n/* */GENERATED ALWAYS AS (" + virtStart
	hashColumnEnd   = virtEnd + ") VIRTUAL"
	now             = "/*! NOW() -- */ DATETIME('now')\n/*! */"
	setRef          = "/*! AS `EXCLUDED` */ "
	updateCol       = "/*! `updated` BOOLEAN DEFAULT FALSE, */"
	colUpdate       = "/*!, `updated` = ! `updated`*/ "

	onConflictUpdate   = "ON /*! DUPLICATE KEY UPDATE -- */ CONFLICT DO UPDATE SET\n/*! */ "
	onConflictReturnID = "ON /*! DUPLICATE KEY UPDATE `id` = LAST_INSERT_ID(`id`); -- */ " +
		"CONFLICT DO UPDATE SET `id` = `id` RETURNING `id`;\n/*! */"
	returnOrSetID = "/*! `id` = LAST_INSERT_ID(`id`); -- */ RETURNING `id`;\n/*! */"

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
		"`metadata` " +
		") VALUES (?, ?, ?, ?, ?, '', '', '');"
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
		onConflictUpdate + "`mtime` = `EXCLUDED`.`mtime`, `dest` = `EXCLUDED`.`dest`, " +
		"`size` = `EXCLUDED`.`size`, `owner` = `EXCLUDED`.`owner` " + colUpdate +
		"/*! , */" + returnOrSetID
	createRemoteFile = "INSERT INTO `remoteFiles` (" +
		"`remotePath`, " +
		"`hardlinkID`, " +
		"`lastError`" +
		") VALUES (?, ?, '') " + onConflictReturnID
	createSetFile = "INSERT INTO `localFiles` (" +
		"`localPath`, " +
		"`setID`, " +
		"`remoteFileID`" +
		") VALUES (?, ?, ?) " + onConflictReturnID
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
	createProcess = "INSERT INTO `processes` (`lastPing`) VALUES (" + now + ");"

	getSetsStart = "SELECT " +
		"`sets`.`id`, " +
		"`sets`.`name`, " +
		"`sets`.`requester`, " +
		"`sets`.`description`, " +
		"`sets`.`monitorTime`, " +
		"`sets`.`numFiles`," +
		"`sets`.`sizeFiles`," +
		"`sets`.`startedDiscovery`, " +
		"`sets`.`lastDiscovery`, " +
		"`sets`.`status`, " +
		"`sets`.`lastCompletedCount`, " +
		"`sets`.`lastCompletedSize`, " +
		"`sets`.`error`, " +
		"`sets`.`warning`, " +
		"`sets`.`readonly`, " +
		"`transformers`.`transformer` " +
		"FROM `sets` JOIN `transformers` ON `sets`.`transformerID` = `transformers`.`id`"
	getAllSets            = getSetsStart + " ORDER BY `sets`.`id` ASC;"
	getSetByNameRequester = getSetsStart +
		" WHERE `sets`.`nameHash` = " + virtPosition + " and `sets`.`requesterHash` = " + virtPosition + ";"
	getSetsByRequester = getSetsStart +
		" WHERE `sets`.`requesterHash` = " + virtPosition + " ORDER BY `sets`.`id` ASC;"
	getSetsFiles = "SELECT " +
		"`localFiles`.`id`, " +
		"`localFiles`.`localPath`, " +
		"`remoteFiles`.`remotePath`, " +
		"`remoteFiles`.`lastUploaded`, " +
		"`hardlinks`.`size`, " +
		"`hardlinks`.`fileType`, " +
		"`hardlinks`.`owner`, " +
		"`hardlinks`.`inode`, " +
		"`hardlinks`.`mountPoint`, " +
		"`hardlinks`.`btime`, " +
		"`hardlinks`.`mtime`, " +
		"`hardlinks`.`remote`, " +
		"`hardlinks`.`dest` " +
		"FROM `localFiles` " +
		"JOIN `remoteFiles` ON `localFiles`.`remoteFileID` = `remoteFiles`.`id` " +
		"JOIN `hardlinks` ON `remoteFiles`.`hardlinkID` = `hardlinks`.`id` " +
		"WHERE `localFiles`.`setID` = ? ORDER BY `localFiles`.`id` ASC;"
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

	updateSetWarning             = "UPDATE `set` SET `warning` = ? WHERE `id` = ?;"
	updateSetError               = "UPDATE `set` SET `warning` = ? WHERE `id` = ?;"
	updateDiscoveryStarted       = "UPDATE `set` SET `startedDiscovery` = ? WHERE `id` = ?;"
	updateLastDiscoveryCompleted = "UPDATE `set` SET " +
		"`lastDiscovery` = `startedDiscovery`, " +
		"`lastCompleted` = ?, " +
		"`lastCompletedCount` = ?, " +
		"`lastCompletedSize` = ? " +
		"WHERE `id` = ?;"
	updateQueueReset   = "UPDATE `queue` set `heldBy` = 0;"
	updateQueuedFailed = "UPDATE `queue` SET " +
		"`attempts` = `attempts` + 1, " +
		"`lastError` = ?, " +
		"`lastAttempt` = " + now + ", " +
		"`heldBy` = 0 " +
		"WHERE `id` = ? AND `type` = ?;"
	updateProcessPing = "UPDATE `processes SET `lastPing` = " + now + " WHERE `id` = ?;"

	holdQueuedTask = "WITH " +
		"`heldHardlinks` AS (" +
		"SELECT `remoteFiles`.`hardlinkID` FROM `queue` " +
		"JOIN `localFiles` ON `queue`.`localFileID` = `localFiles`.`id` " +
		"JOIN `remoteFiles` ON `localFiles`.`remoteFileID` = `remoteFiles`.`id` " +
		"WHERE `queue`.`heldBy` != 0" +
		"), " +
		"`available` AS (" +
		"SELECT `queue`.`id` FROM `queue` " +
		"JOIN `localFiles` ON `queue`.`localFileID` = `localFiles`.`id` " +
		"JOIN `remoteFiles` ON `localFiles`.`remoteFileID` = `remoteFiles`.`id` " +
		"WHERE `remoteFiles`.`hardlinkID` NOT IN (SELECT `hardlinkID` FROM `heldHardlinks`) " +
		"ORDER BY `queue`.`attempts` ASC, `queue`.`id` ASC LIMIT 1" +
		") " +
		"UPDATE `queue` SET `heldBy` = ? WHERE " +
		"`queue`.`id` IN (SELECT `id` FROM `available`);"
	releaseQueuedTask = "UPDATE `queue` SET `heldBy` = 0 WHERE `heldBy` = ?;"

	deleteSet            = "DELETE FROM `sets` WHERE `id` = ?;"
	deleteDiscover       = "DELETE FROM `toDiscover` WHERE `setID` = ? AND `path` = ?;"
	deleteQueued         = "DELETE FROM `queue` WHERE `localFileID` = ? AND `type` = ?;"
	deleteAllQueued      = "DELETE FROM `queue`;"
	deleteStaleProcesses = "DELETE FROM `processes` WHERE `lastPing` < /* NOW() - 10 MINUTE -- */ " +
		"DATETIME('now', '-10 MINUTE')\n/*! */;"
)
