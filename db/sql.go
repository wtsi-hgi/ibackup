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
			"`error` TEXT, " +
			"`warning` TEXT, " +
			"`metadata` TEXT, " +
			"`deleteLocal` BOOLEAN DEFAULT FALSE, " +
			"`readonly` BOOLEAN DEFAULT FALSE, " +
			"UNIQUE(`requesterHash`, `nameHash`), " +
			"FOREIGN KEY(`transformerID`) REFERENCES `transformers`(`id`) ON UPDATE RESTRICT ON DELETE RESTRICT" +
			");",
		"CREATE TABLE IF NOT EXISTS `toDiscover` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`setID` INTEGER NOT NULL, " +
			"`path` TEXT NOT NULL, " +
			"`type` TINYINT NOT NULL, " +
			"FOREIGN KEY(`setID`) REFERENCES `sets`(`id`) ON UPDATE RESTRICT ON DELETE CASCADE" +
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
			"`dest` TEXT NOT NULL, " +
			"`remote` TEXT NOT NULL, " +
			"UNIQUE(`mountpointHash`, `inode`, `btime`)" +
			");",
		"CREATE TABLE IF NOT EXISTS `remoteFiles` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`remotePath` TEXT NOT NULL, " +
			"`remotePathHash` " + hashColumnStart + "`remotePath`" + hashColumnEnd + ", " +
			//"`status` TINYINT NOT NULL, " +
			"`lastUploaded` DATETIME DEFAULT \"0001-01-01 00:00:00\", " +
			"`lastError` TEXT NOT NULL, " +
			"`hardlinkID` INTEGER NOT NULL, " +
			"UNIQUE(`remotePathHash`), " +
			"FOREIGN KEY(`hardlinkID`) REFERENCES `hardlinks`(`id`) ON UPDATE RESTRICT ON DELETE RESTRICT" +
			");",
		"CREATE TABLE IF NOT EXISTS `localFiles` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`localPath` TEXT NOT NULL, " +
			"`localPathHash` " + hashColumnStart + "`localPath`" + hashColumnEnd + ", " +
			"`setID` INTEGER NOT NULL, " +
			"`remoteFileID` INTEGER NOT NULL, " +
			"UNIQUE(`localPathHash`, `setID`), " +
			"FOREIGN KEY(`setID`) REFERENCES `sets`(`id`) ON DELETE CASCADE, " +
			"FOREIGN KEY(`remoteFileID`) REFERENCES `remoteFiles`(`id`) ON UPDATE RESTRICT ON DELETE RESTRICT" +
			");",
		"CREATE TABLE IF NOT EXISTS `uploads` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`localFileID` INTEGER, " +
			"`attempts` INTEGER DEFAULT 0, " +
			"`lastAttempt` DATETIME DEFAULT \"0001-01-01 00:00:00\", " +
			"`lastError` TEXT, " +
			"FOREIGN KEY(`localFileID`) REFERENCES `localFiles`(`id`) ON UPDATE RESTRICT ON DELETE RESTRICT" +
			");",
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
	}
)

const (
	virtStart       = "/*! UNHEX(SHA2(*/"
	virtEnd         = "/*!, 0))*/"
	virtPosition    = virtStart + "?" + virtEnd
	hashColumnStart = "/*! VARBINARY(32) -- */ TEXT\n/* */GENERATED ALWAYS AS (" + virtStart
	hashColumnEnd   = virtEnd + ") VIRTUAL"

	onConflictUpdate   = "ON /*! DUPLICATE KEY UPDATE -- */ CONFLICT DO UPDATE SET\n/*! */ "
	onConflictReturnID = "ON /*! DUPLICATE KEY UPDATE `id` = LAST_INSERT_ID(`id`); -- */ " +
		"CONFLICT DO UPDATE SET `id` = `id` RETURNING `id`;\n/*! */"
	returnOrSetID = "/*! `id` = LAST_INSERT_ID(`id`); -- */ `id` = `id` RETURNING `id`;\n/*! */"

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
		"`dest`" +
		") VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
		onConflictUpdate + "`remote` = ?, `mtime` = ?, `dest` = ?, `size` = ?, " + returnOrSetID
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
		") VALUES (?, ?, ?) " + onConflictReturnID

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
		"`hardlinks`.`size`, " +
		"`hardlinks`.`fileType`, " +
		"`hardlinks`.`dest`, " +
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
		") FROM `toDiscover` WHERE `setID` = ?;"

	updateSetWarning             = "UPDATE `set` SET `warning` = ? WHERE `id` = ?;"
	updateSetError               = "UPDATE `set` SET `warning` = ? WHERE `id` = ?;"
	updateDiscoveryStarted       = "UPDATE `set` SET `startedDiscovery` = ? WHERE `id` = ?;"
	updateLastDiscoveryCompleted = "UPDATE `set` SET " +
		"`lastDiscovery` = `startedDiscovery`, " +
		"`lastCompleted` = ?, " +
		"`lastCompletedCount` = ?, " +
		"`lastCompletedSize` = ? " +
		"WHERE `id` = ?;"

	deleteSet      = "DELETE FROM `sets` WHERE `id` = ?;"
	deleteSetFiles = "DELETE FROM `localFiles` WHERE `setID` = ?;"
	deleteSetFile  = "DELETE FROM `localFiles` WHERE `id` = ?;"
	deleteDiscover = "DELETE FROM `toDiscover` WHERE `setID` = ? AND `path` = ?;"
)
