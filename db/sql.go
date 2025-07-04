//nolint:gochecknoglobals
package db

var (
	tables = [...]string{
		"CREATE TABLE IF NOT EXISTS `transformers` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`transformer` TEXT NOT NULL, " +
			"UNIQUE(`transformer`)" +
			");",
		"CREATE TABLE IF NOT EXISTS `sets` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`name` TEXT NOT NULL, " +
			"`requester` TEXT NOT NULL, " +
			"`transformerID` INTEGER NOT NULL, " +
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
			"`error` TEXT DEFAULT \"\", " +
			"`warning` TEXT  DEFAULT \"\", " +
			"`metadata` TEXT  DEFAULT \"{}\", " +
			"`deleteLocal` BOOLEAN DEFAULT FALSE, " +
			"`readonly` BOOLEAN DEFAULT FALSE, " +
			"UNIQUE(`requester`, `name`), " +
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
			"`btime` INTEGER, " +
			"`size` INTEGER NOT NULL, " +
			"`fileType` TINYINT NOT NULL, " +
			"`dest` TEXT NOT NULL, " +
			"`remote` TEXT NOT NULL, " +
			"UNIQUE(`mountpoint`, `inode`, `btime`)" +
			");",
		"CREATE TABLE IF NOT EXISTS `remoteFiles` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`remotePath` TEXT NOT NULL, " +
			"`status` TINYINT NOT NULL, " +
			"`lastUploaded` DATETIME DEFAULT \"0001-01-01 00:00:00\", " +
			"`lastError` TEXT DEFAULT \"\", " +
			"`hardlinkID` INTEGER NOT NULL, " +
			"UNIQUE(`remotePath`), " +
			"FOREIGN KEY(`hardlinkID`) REFERENCES `hardlinks`(`id`) ON UPDATE RESTRICT ON DELETE RESTRICT" +
			");",
		"CREATE TABLE IF NOT EXISTS `localFiles` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`localPath` TEXT NOT NULL, " +
			"`setID` INTEGER NOT NULL, " +
			"`remoteFileID` INTEGER NOT NULL, " +
			"UNIQUE(`localPath`, `setID`), " +
			"FOREIGN KEY(`setID`) REFERENCES `sets`(`id`) ON DELETE CASCADE, " +
			"FOREIGN KEY(`remoteFileID`) REFERENCES `remoteFiles`(`id`) ON UPDATE RESTRICT ON DELETE RESTRICT" +
			");",
		"CREATE TABLE IF NOT EXISTS `uploads` (" +
			"`id` INTEGER PRIMARY KEY /*! AUTO_INCREMENT */, " +
			"`localFileID` INTEGER, " +
			"`attempts` INTEGER DEFAULT 0, " +
			"`lastAttempt` DATETIME DEFAULT \"0001-01-01 00:00:00\", " +
			"`lastError` TEXT DEFAULT \"\", " +
			"FOREIGN KEY(`localFileID`) REFERENCES `localFiles`(`id`) ON UPDATE RESTRICT ON DELETE RESTRICT" +
			");",
		"CREATE TRIGGER IF NOT EXISTS `insert_file_count_size` AFTER INSERT ON `localFiles` FOR EACH ROW BEGIN " +
			"UPDATE `sets` SET `numFiles` = `numFiles` + 1, `sizeFiles` = `sizeFiles` + (" +
			"SELECT `hardlinks`.`size` FROM `hardlinks` JOIN `remoteFiles` ON `remoteFiles`.`hardlinkID` = `hardlinks`.`id` " +
			"WHERE `remoteFiles`.`id` = `NEW`.`remoteID`) " +
			"WHERE `id` = `NEW`.`setID`; END;",
		"CREATE TRIGGER IF NOT EXISTS `delete_file_count_size` AFTER DELETE ON `localFiles` FOR EACH ROW BEGIN " +
			"UPDATE `sets` SET `numFiles` = `numFiles` - 1, `sizeFiles` = `sizeFiles` - (" +
			"SELECT `hardlinks`.`size` FROM `hardlinks` JOIN `remoteFiles` ON `remoteFiles`.`hardlinkID` = `hardlinks`.`id` " +
			"WHERE `remoteFiles`.`id` = `NEW`.`remoteID`) " +
			"WHERE `id` = `OLD`.`setID`;" +
			"END;",
		"CREATE TRIGGER IF NOT EXISTS `update_file_size` AFTER UPDATE ON `hardlinks` FOR EACH ROW BEGIN " +
			"UPDATE `sets` SET `sizeFiles` = `sizeFiles` - `OLD`.`size` + `NEW`.`size` WHERE `id` IN (" +
			"SELECT `localFiles`.`setID` FROM `localFiles` " +
			"JOIN `remoteFiles` ON `remoteFiles`.`id` = `localFiles`.`remoteID` WHERE " +
			"`remoteFiles`.`hardlinkID` = `NEW`.`id`);" +
			"END;",
	}
)

const (
	onConflictUpdate   = "ON /*! DUPLICATE KEY UPDATE --*/ CONFLICT DO UPDATE SET\n"
	onConflictReturnID = "ON /*! DUPLICATE KEY UPDATE DO NOTHING; SELECT LAST_INSERT_ID();--*/ " +
		"CONFLICT DO UPDATE SET `id` = `id` RETURNING `id`\n;"
	createTransformer = "INSERT INTO `transformers` (" +
		"`transformer`" +
		") VALUES (?) " + onConflictReturnID
	createSet = "INSERT INTO `sets` (" +
		"`name`, " +
		"`requester`, " +
		"`transformerID`, " +
		"`monitorTime`, " +
		"`description`" +
		") VALUES (?, ?, ?, ?, ?);"
	createHardlink = "INSERT INTO `hardlinks` (" +
		"`inode`, " +
		"`mountpoint`, " +
		"`btime`, " +
		"`remote`" +
		"`mtime`, " +
		"`size`, " +
		"`fileType`, " +
		"`dest`, " +
		") VALUES (?, ?, ?, ?, ?, ?, ?, ?) " + onConflictUpdate + "`remote` = ?, `mtime` = ?, `dest` = ?;"
	createRemoteFile = "INSERT INTO `remoteFiles` (" +
		"`remotePath`, " +
		"`hardlinkID`" +
		") VALUES (?, ?) " + onConflictReturnID
	createSetFile = "INSERT INTO `setFiles` (" +
		"`localPath`, " +
		"`setID`, " +
		"`remoteFilesID`" +
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
	getAllSets            = getSetsStart + ";"
	getSetByNameRequester = getSetsStart +
		" WHERE `sets`.`name` = ? and `sets`.`requester` = ?;"
	getSetsByRequester = getSetsStart +
		" WHERE `requester` = ?;"
	getSetsFiles = "SELECT " +
		"`localFiles`.`id`, " +
		"`localFiles`.`localPath`, " +
		"`remoteFiles`.`remotePath`" +
		"`hardlinks`.`size`" +
		"`hardlinks`.`fileType`" +
		"`hardlinks`.`dest`" +
		"`hardlinks`.`inode`" +
		"`hardlinks`.`mountPoint`" +
		"`hardlinks`.`btime`" +
		"`hardlinks`.`remote` " +
		"FROM `localFiles` " +
		"JOIN `remoteFiles` ON `localFiles`.`remoteFileID` = `remoteFiles`.`id` " +
		"JOIN `hardlinks` ON `remoteFiles`.`hardlinkID` = `hardlinks`.`id` " +
		"WHERE `localFiles`.`setID` = ?"
	getSetDiscovery = "SELECT " +
		"`path`, " +
		"`type`" +
		") FROM `toDiscover` WHERE `setID` = ?;"
	updateSetWarning             = "UPDATE `set` SET `warning` = ? WHERE `id` = ?;"
	updateSetError               = "UPDATE `set` SET `warning` = ? WHERE `id` = ?;"
	updateDiscoveryStarted       = "UPDATE `set` SET `startedDiscovery` = ? WHERE `is` = ?;"
	updateLastDiscoveryCompleted = "UPDATE `set` SET " +
		"`lastDiscovery` = `startedDiscovery`, " +
		"`lastCompleted` = ?, " +
		"`lastCompletedCount` = ?, " +
		"`lastCompletedSize` = ? " +
		"WHERE `id` = ?;"
	deleteSet      = "DELETE FROM `sets` WHERE `id` = ?;"
	deleteSetFiles = "DELETE FROM `setFiles` WHERE `setID` = ?;"
	deleteSetFile  = "DELETE FROM `setFiles` WHERE `id` = ? AND `setID` = ?;"
	deleteDiscover = "DELETE FROM `toDiscover` WHERE `setID` = ? AND `path` = ?;"
)
