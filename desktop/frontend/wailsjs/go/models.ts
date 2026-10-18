export namespace datalake {
	
	export class Stats {
	    totalFiles: number;
	    processedFiles: number;
	    failedFiles: number;
	    failures: Record<string, string>;
	
	    static createFrom(source: any = {}) {
	        return new Stats(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.totalFiles = source["totalFiles"];
	        this.processedFiles = source["processedFiles"];
	        this.failedFiles = source["failedFiles"];
	        this.failures = source["failures"];
	    }
	}

}

export namespace model {
	
	export class SyncLog {
	    id?: string;
	    fileName?: string;
	    fileHash?: string;
	    collectionName: string;
	    recordCount?: number;
	    upsertedCount?: number;
	    matchedCount?: number;
	    status?: string;
	    errorMessage?: string;
	    // Go type: time
	    syncedAt?: any;
	    durationMs?: number;
	    // Go type: time
	    syncTimestamp?: any;
	    recordsUploaded?: number;
	
	    static createFrom(source: any = {}) {
	        return new SyncLog(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.fileName = source["fileName"];
	        this.fileHash = source["fileHash"];
	        this.collectionName = source["collectionName"];
	        this.recordCount = source["recordCount"];
	        this.upsertedCount = source["upsertedCount"];
	        this.matchedCount = source["matchedCount"];
	        this.status = source["status"];
	        this.errorMessage = source["errorMessage"];
	        this.syncedAt = this.convertValues(source["syncedAt"], null);
	        this.durationMs = source["durationMs"];
	        this.syncTimestamp = this.convertValues(source["syncTimestamp"], null);
	        this.recordsUploaded = source["recordsUploaded"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}

}

